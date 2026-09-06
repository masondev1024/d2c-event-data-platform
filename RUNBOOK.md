# D2C 이벤트 파이프라인 운영 런북

이 런북은 다음 불변식을 운영자가 확인하고 장애 시 복구하는 절차를 정의합니다.

```text
응모 승인 API
  └─ 같은 PostgreSQL 트랜잭션
       ├─ d2c_applications = APPROVED
       └─ d2c_outbox_events = d2c.application.approved.v1
              └─ outbox publisher (at-least-once)
                   └─ Kafka
                        └─ idempotent consumer (event_id PK)
                             └─ DuckDB operational sink / lake export boundary

Prometheus parity + 5xx + p95 + backlog
  └─ Argo Rollouts AnalysisRun
       ├─ pass: canary promotion
       └─ fail: abort and stable service remains traffic target
```

## 운영 불변식과 gate

| 신호 | 정상 기준 | 실패 시 의미 |
| --- | ---: | --- |
| `d2c_outbox_parity_gap` | `0` | 승인 row와 outbox row의 DB truth가 어긋남 |
| `d2c_outbox_parity_check_success` | `1` | parity 조회 자체가 성공해야 함; 조회 실패는 fail-closed |
| `/api/apply` 5xx ratio | `< 1%` | canary가 승인 요청을 안정적으로 처리하지 못함 |
| `/api/apply` p95 | `< 500 ms` | DB/connection pool/리소스 지연 가능성 |
| `d2c_outbox_backlog` | `< 100` | publisher 또는 Kafka가 drain하지 못함 |
| `d2c_apply_requests_total{result="success"}` | `> 0` | 트래픽 없는 canary를 통과시키지 않음 |

`d2c_outbox_parity_gap`은 counter가 아니라 매 `/metrics` scrape 때 PostgreSQL의 승인
row와 outbox row를 직접 비교한 gauge입니다. 따라서 애플리케이션 프로세스가 “성공”이라고
기록한 counter보다 release gate에 적합합니다. publisher는 Kafka ack 후 `published_at`을
기록하므로, ack와 DB update 사이 crash는 중복 publish를 만들 수 있습니다. consumer는
`event_id` primary key와 `ON CONFLICT DO NOTHING`으로 이를 흡수합니다. 이 경계는
exactly-once가 아니라 at-least-once + idempotent sink입니다.

## 로컬 수직 슬라이스

```bash
docker compose --profile d2c up -d --build \
  kafka kafka-init postgres d2c-migrate d2c-api \
  d2c-outbox-publisher d2c-event-consumer
```

마이그레이션과 health 상태를 확인합니다.

```bash
docker compose --profile d2c ps -a
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

승인과 idempotency replay를 실행합니다.

```bash
curl -sS -X POST http://127.0.0.1:8080/api/apply \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: apply-local-0001' \
  -d '{"user_id":1001,"campaign_id":1}'

# 같은 key와 aggregate replay는 두 번째 outbox를 만들지 않습니다.
curl -sS -X POST http://127.0.0.1:8080/api/apply \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: apply-local-0001' \
  -d '{"user_id":1001,"campaign_id":1}'
```

DB truth, publish 상태, consumer의 deduplicated 저장, Kafka lag를 함께 확인합니다.

```bash
docker compose exec -T postgres psql -U d2c -d d2c -c \
  'SELECT COUNT(*) AS applications,
          (SELECT COUNT(*) FROM d2c_outbox_events) AS outbox_events,
          (SELECT COUNT(*) FROM d2c_outbox_events WHERE published_at IS NOT NULL) AS published,
          (SELECT COUNT(*) FROM d2c_outbox_events WHERE published_at IS NULL) AS backlog;'

curl -fsS http://127.0.0.1:8080/metrics | rg \
  'd2c_(apply_requests_total|outbox_events_total|outbox_parity_gap|outbox_parity_check_success|db_readiness)'

docker compose exec -T d2c-event-consumer python -c \
  "import duckdb; c=duckdb.connect('/data/d2c_events.duckdb', read_only=True); print(c.execute('select count(*), count(distinct event_id) from d2c_application_events').fetchone())"

docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe --group d2c-application-event-consumer-v1
```

## 장애 드릴

### 1. Outbox insert 실패: 원자성 보호

validation 환경에서만 주입 플래그를 켭니다. 기본 환경에서는 주입이 동작하지 않습니다.

```bash
D2C_ENV=validation \
ALLOW_FAILURE_DRILL=true \
D2C_OUTBOX_FAILURE_INJECTION=before_outbox_insert \
docker compose --profile d2c up -d --force-recreate d2c-api

curl -i -X POST http://127.0.0.1:8080/api/apply \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: apply-failure-drill-0001' \
  -d '{"user_id":2001,"campaign_id":1}'
```

기대 결과는 HTTP 503이며, `d2c_applications`와 `d2c_outbox_events`가 모두 증가하지
않고 parity가 `0`인 것입니다. 검증 후 반드시 기본 환경으로 되돌립니다.

```bash
docker compose --profile d2c up -d --force-recreate d2c-api
curl -fsS http://127.0.0.1:8080/readyz
```

### 2. Publisher 중단: backlog recovery

```bash
docker compose --profile d2c stop d2c-outbox-publisher

curl -sS -X POST http://127.0.0.1:8080/api/apply \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: apply-publisher-outage-0001' \
  -d '{"user_id":2003,"campaign_id":1}'

# 이 시점에 published_at IS NULL row가 생겨야 합니다.
docker compose --profile d2c start d2c-outbox-publisher
sleep 3

# backlog=0, consumer count 증가, Kafka lag=0으로 수렴해야 합니다.
```

### 3. PostgreSQL 또는 Kafka 장애

PostgreSQL 중단 시 API readiness와 parity check가 fail-closed인지 확인합니다. Kafka 중단
시에는 API 트랜잭션은 승인과 outbox row를 보존하고 publisher가 retry/backlog 상태가 되어야
합니다. 복구 후 outbox backlog와 Kafka consumer lag가 0으로 수렴하는지 확인합니다.

```bash
docker compose --profile d2c stop postgres
curl -i http://127.0.0.1:8080/readyz
curl -fsS http://127.0.0.1:8080/metrics | rg \
  'd2c_(db_readiness|outbox_parity_gap|outbox_parity_check_success)'
docker compose --profile d2c start postgres

docker compose --profile d2c stop kafka
docker compose --profile d2c logs --tail=80 d2c-outbox-publisher
docker compose --profile d2c start kafka
```

### 4. Argo canary abort

기본 매니페스트는 `k8s/d2c/base`, 실패 주입 overlay는
`k8s/d2c/overlays/validation`입니다. validation overlay는 모든 Rollout template에
failure drill을 넣으므로 정상 운영 배포에 사용하지 않습니다.

```bash
kubectl apply -k k8s/d2c/overlays/validation
kubectl argo rollouts get rollout d2c-api -n d2c --watch
kubectl get analysisrun -n d2c
kubectl describe analysisrun -n d2c
```

기대 결과는 canary AnalysisRun이 parity/5xx/traffic 조건 중 하나로 실패하고 Rollout이
abort 또는 degraded 상태가 되며, stable service가 계속 요청을 처리하는 것입니다. 실제
검증에는 Argo Rollouts controller, NGINX Ingress, Prometheus Operator, Kafka, PostgreSQL,
`d2c-api-secret`, `d2c-api-tls`가 필요합니다. 현재 repository에서는 매니페스트 render와
local Docker 수직 슬라이스까지 자동 검증하며, 클러스터가 없는 환경에서 실제 Rollout
상태를 성공했다고 주장하지 않습니다.

## 로컬 Parquet lake export

기존 sensor canonical DuckDB sink는 별도의 event-time Parquet export 경계를 가집니다.
sink가 파일을 쓰는 동안 DuckDB 단일 writer lock이 있으므로 export 전 sink를 멈춥니다.

```bash
docker compose stop stream-sink
python3 scripts/export_sensor_duckdb_to_lake.py \
  --database data/sensor.duckdb \
  --output data/lake \
  --batch-id sensor-$(date -u +%Y%m%dT%H%M%SZ)
python3 scripts/validate_lake_contract.py --path data/lake
docker compose start stream-sink
```

local lake는 event-time partition과 Kafka source lineage를 보존하고, AWS bronze는
Firehose 도착일 partition이라는 차이가 있습니다. 대규모 운영 전환에서는 이 경계를
Spark Structured Streaming + Iceberg snapshot/compaction으로 교체합니다.

## AWS 실습 종료

AWS path는 README의 Terraform/Firehose 절차를 따릅니다. 실습 종료 후 다음 순서로
Docker와 Terraform 리소스를 정리합니다.

```bash
docker compose --profile aws --profile observability down --remove-orphans
cd infra/terraform
terraform destroy
```

versioned S3 bucket의 object version이 남아 있으면 `force_destroy=false` 때문에 destroy가
실패합니다. 삭제 전 `aws s3api list-object-versions`로 current/non-current version과 delete
marker를 확인하고, 필요한 결과만 명시적으로 삭제합니다. 상시 MSK/EKS를 만들지 않는 현재
구성은 포트폴리오 일회성 검증에서 비용과 잔존 리소스를 줄이는 선택입니다.

## 2026-09-06 로컬 검증 기록

실제 로컬 Docker 수직 슬라이스에서 확인한 값입니다.

| 항목 | 결과 |
| --- | ---: |
| 승인 API 성공 | 9건 |
| PostgreSQL applications / outbox | 9 / 9 |
| published / unpublished outbox | 9 / 0 |
| consumer stored / unique event_id | 9 / 9 |
| Kafka lag | 모든 partition 0 |
| parity gap / check success | 0 / 1 |
| 승인 응답 sample | 11.6–19.1 ms (초기 5건) |
| failure drill | 503, DB row 증가 0 |
| publisher outage recovery | backlog 1 → 재기동 후 0 |
| PostgreSQL outage | readiness 503, parity gap/check 1/0 → 복구 |
| Kafka outage | 승인 201, backlog 1 → broker 복구 후 0 |

초기 publisher 이미지에서 `kafka-python` 의존성이 빠져 process가 재시작하는 문제가
발견됐습니다. `app/requirements.txt`에 고정 버전을 추가하고 이미지를 재빌드한 뒤
기존 outbox row가 재처리되어 전체 수직 슬라이스가 정상 수렴했습니다. 이 기록은
“green path만 실행했다”가 아니라 failure symptom → root cause → recovery evidence를
남긴 사례입니다.
