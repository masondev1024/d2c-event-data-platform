"""Fail closed if the source-derived runtime assembly drifts from upstream rules."""
from pathlib import Path
import hashlib
import json
import re


def block(text, marker):
    position = text.index(marker) + len(marker)
    opening = text.index('{', position)
    depth = 1
    for end in range(opening + 1, len(text)):
        if text[end] == '{':
            depth += 1
        elif text[end] == '}':
            depth -= 1
            if depth == 0:
                return text[opening + 1:end]
    raise RuntimeError(f'Unclosed upstream build block: {marker}')


source = Path('/upstream/spark/v4.1/build.gradle').read_text()
recipe = Path('/assembly/build.gradle').read_text()
runtime = source[source.index('project(":iceberg-spark:iceberg-spark-runtime-${sparkMajorVersion}_${scalaVersion}") {'):]
relocations = lambda text: re.findall(r"relocate\s+'([^']+)',\s*'([^']+)'", text)
if relocations(runtime) != relocations(recipe):
    raise RuntimeError('Runtime relocation rules differ from the pinned upstream source')
exclusions = lambda text: re.findall(r"exclude group:\s*'([^']+)'(?:,\s*module:\s*'([^']+)')?", text)
expected_exclusions = exclusions(block(runtime, 'implementation'))
actual_exclusions = exclusions(block(recipe, 'configurations.implementation'))
if expected_exclusions != actual_exclusions:
    raise RuntimeError('Spark-provided runtime exclusion rules differ from upstream')
dependencies = block(runtime, 'dependencies').split('integrationImplementation', 1)[0]
dependencies = dependencies.replace('${sparkMajorVersion}', '4.1').replace('${scalaVersion}', '2.13')
expected_modules = [path.rsplit(':', 1)[-1] for path in re.findall(
    r'''(?:api|implementation)\s*\(?\s*project\((?:path:\s*)?['"]([^'"]+)['"]''', dependencies)]
dependency_recipe = recipe[recipe.index('configurations.implementation'):]
actual_modules = re.findall(r'''(?:api|implementation)\s*\(?\s*['"]org\.apache\.iceberg:([^:]+):1\.12\.0['"]''', block(dependency_recipe, 'dependencies'))
if expected_modules != actual_modules:
    raise RuntimeError(f'Runtime component list differs: {expected_modules} != {actual_modules}')
if list(Path('/upstream/spark/v4.1/spark-runtime/src/main').glob('**/*')):
    raise RuntimeError('Upstream runtime gained main sources; a packaging-only rebuild requires review')
provenance = {
    'component': 'iceberg-spark-runtime-4.1_2.13',
    'upstream_version': '1.12.0',
    'custom_version': '1.12.0-d2c-security1',
    'source_tag': 'apache-iceberg-1.12.0',
    'source_commit': '212aa894e919ff9fdf89160da4aadcbe6858ac01',
    'source_url': 'https://codeload.github.com/apache/iceberg/tar.gz/refs/tags/apache-iceberg-1.12.0',
    'source_sha256': '6dfa5e1e5a47061b070a9c265f8006afa3bf5e549e129700375f91c290921811',
    'recipe_sources': ['spark/v4.1/build.gradle', 'build.gradle'],
    'upstream_runtime_gradle_sha256': hashlib.sha256(source.encode()).hexdigest(),
    'source_derived_assembly_sha256': hashlib.sha256(recipe.encode()).hexdigest(),
    'gradle_version': '8.14.5',
    'gradle_distribution_sha256': '6f74b601422d6d6fc4e1f9a1ab6522f642c2fdcbc15ae33ebd30ba3d7198e854',
    'gradle_wrapper_sha256': '7d3a4ac4de1c32b59bc6a4eb8ecb8e612ccd0cf1ae1e99f66902da64df296172',
    'shadow_plugin': 'com.gradleup.shadow:shadow-gradle-plugin:8.3.11',
    'unchanged_published_iceberg_components': actual_modules,
    'verified_relocation_count': len(relocations(recipe)),
    'patched_dependencies': {
        'jackson-core': '2.21.7', 'jackson-databind': '2.21.7',
        'jackson-annotations': '2.21', 'jackson-datatype-jsr310': '2.21.7',
        'parquet-jackson': '1.17.1-d2c-jackson2.21.7',
        'at.yawk.lz4:lz4-java': '1.11.4',
    },
    'original_fat_jar_usage': 'comparison oracle only; never an assembly dependency or input',
    'original_fat_jar_sha256': '3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82',
    'binary_edits': False,
}
Path('/artifacts').mkdir(exist_ok=True)
Path('/artifacts/iceberg-source-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
print('Pinned upstream runtime assembly rule verification PASS')
