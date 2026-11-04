from pathlib import Path
import argparse
import hashlib
import urllib.request

inputs = {
    'iceberg-source.tar.gz': (
        'https://codeload.github.com/apache/iceberg/tar.gz/refs/tags/apache-iceberg-1.12.0',
        '6dfa5e1e5a47061b070a9c265f8006afa3bf5e549e129700375f91c290921811',
    ),
    'gradle-wrapper.jar': (
        'https://raw.githubusercontent.com/gradle/gradle/v8.14.5/gradle/wrapper/gradle-wrapper.jar',
        '7d3a4ac4de1c32b59bc6a4eb8ecb8e612ccd0cf1ae1e99f66902da64df296172',
    ),
    'iceberg-runtime-original.jar': (
        'https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar',
        '3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82',
    ),
}
def fetch_inputs(destination, *, source_only=False):
    destination.mkdir(parents=True, exist_ok=True)
    for name, (url, expected) in inputs.items():
        if source_only and name == 'iceberg-runtime-original.jar':
            continue
        path = destination / name
        if path.exists():
            data = path.read_bytes()
        else:
            with urllib.request.urlopen(url, timeout=90) as response:
                data = response.read()
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected:
            raise RuntimeError(f'{name}: SHA256 {actual}, expected {expected}')
        if not path.exists():
            path.write_bytes(data)
        print(f'{name} SHA256={actual}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-only', action='store_true',
                        help='Fetch no runtime fat JAR; Spark 4.2 is built from source')
    parser.add_argument('--destination', type=Path, default=Path('/verified-inputs'))
    args = parser.parse_args()
    fetch_inputs(args.destination, source_only=args.source_only)
