.PHONY: test verify schema-check evidence-check quality-check d2c-contract-check lake-export catalog-check lakehouse-check lakehouse-security-check lakehouse-image

PYTHON ?= python3

test:
	$(PYTHON) -m unittest discover -s tests -v

schema-check:
	$(PYTHON) scripts/check_schema_compatibility.py \
		--baseline schemas/factory-sensor.v1.schema.json \
		--candidate schemas/factory-sensor.v1.next.schema.json

evidence-check:
	$(PYTHON) scripts/validate_pipeline_evidence.py \
		--manifest examples/pipeline-evidence.ci.json

quality-check:
	@test -n "$(LAKE_PATH)" || (echo "LAKE_PATH is required" >&2; exit 2)
	$(PYTHON) scripts/validate_lake_contract.py --path "$(LAKE_PATH)"

d2c-contract-check:
	$(PYTHON) -m unittest discover -s tests -p 'test_d2c_contract.py' -v
	$(PYTHON) -m unittest discover -s tests -p 'test_d2c_consumer_contract.py' -v

lake-export:
	@test -n "$(BATCH_ID)" || (echo "BATCH_ID is required" >&2; exit 2)
	$(PYTHON) scripts/export_sensor_duckdb_to_lake.py --batch-id "$(BATCH_ID)"

catalog-check:
	$(PYTHON) scripts/validate_catalog.py --catalog-dir catalog/data-products --repository-root .

lakehouse-check:
	$(PYTHON) -m unittest discover -s tests -p 'test_d2c_iceberg_stream.py' -v

lakehouse-security-check:
	$(PYTHON) scripts/validate_lakehouse_trivy_exceptions.py \
		--policy security/lakehouse.trivyignore.yaml

lakehouse-image:
	# Set D2C_DOCKER_STORAGE_PATH to monitor Docker's backing filesystem too.
	$(PYTHON) -c 'import hashlib, os, sys, tempfile, uuid; from pathlib import Path; \
		root = Path.cwd().resolve(); temporary = Path(tempfile.gettempdir()).resolve(); \
		private = (temporary / ("d2c-container-security-" + hashlib.sha256(os.fsencode(root)).hexdigest()[:24])).resolve(); \
		state = (private / "build-state").resolve(); evidence = (private / "evidence" / uuid.uuid4().hex).resolve(); \
		any(path.is_relative_to(root) for path in (private, state, evidence)) and sys.exit("temporary build state must be outside the build context"); \
		arguments = [sys.executable, str(root / "scripts/container_security_guard.py"), \
			"--kind", "build", "--minimum-free-gib", "20", "--reserve-free-gib", "6", \
			"--timeout-seconds", "1800", "--build-state-dir", str(state), \
			"--evidence-dir", str(evidence), \
			"--storage-path", str(root), "--storage-path", str(temporary)]; \
		extra = os.environ.get("D2C_DOCKER_STORAGE_PATH"); \
		arguments += ["--storage-path", extra] if extra else []; \
		os.execv(sys.executable, arguments + ["--", "docker", "build", "--file", \
			"lakehouse/Dockerfile", "--tag", "d2c-iceberg-stream:local", "."])'

verify: test schema-check evidence-check d2c-contract-check catalog-check lakehouse-security-check
	$(PYTHON) -m compileall -q app services scripts lakehouse d2c_contract.py log_gen.py
