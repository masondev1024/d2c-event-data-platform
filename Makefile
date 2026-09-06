.PHONY: test verify schema-check evidence-check quality-check d2c-contract-check lake-export

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

verify: test schema-check evidence-check d2c-contract-check
	$(PYTHON) -m compileall -q app services scripts d2c_contract.py log_gen.py
