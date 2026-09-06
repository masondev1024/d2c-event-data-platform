# AWS lakehouse lab

이 Terraform 모듈은 로컬 Kafka에서 검증된 `factory.sensor.clean.v1` 이벤트를
Vector → Kinesis Data Firehose → S3 bronze Parquet로 전달하기 위한 최소 운영 경계를
구성합니다. Cloud bronze 경로는 Firehose 도착 시각 기준 `ingest_year/month/day`로
파티션합니다. event-time 기반 local lab partition과 의미가 다르므로 이를 명시적으로
분리했습니다.

생성 리소스:

- 공개 접근 차단·고객 관리형 KMS(SSE-KMS) 암호화·버전관리·짧은 lifecycle을 적용한 S3 bucket
- 키 자동 교체가 활성화된 customer-managed KMS key와 Firehose 전용 암호화 권한
- JSON → Snappy Parquet 변환이 가능한 DirectPut Firehose
- Firehose가 참조할 Glue Catalog database/table과 Athena partition projection
- Firehose 전용 least-privilege IAM role/policy
- Firehose delivery 진단용 CloudWatch Log Group(기본 7일 보존)

## 안전한 실행 순서

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -out=tfplan
terraform apply tfplan
terraform output
```

기본값은 포트폴리오용 짧은 실습을 전제로 합니다. `terraform apply`는 AWS 계정에 실제
리소스를 생성하고 비용이 발생할 수 있으므로, 로컬 검증 단계에서는 실행하지 않습니다.

`enable_parquet_conversion = true`인 경우 Firehose의 변환 버퍼 최소 크기가 64 MiB이고
기본 flush interval은 60초입니다. 짧은 테스트에서는 S3 object가 즉시 보이지 않는 것이
정상이며, 작은 이벤트로 즉시 확인해야 하면 일시적으로 `false`로 바꿔 JSON/GZIP landing을
검증한 뒤 Parquet 모드로 최종 확인합니다.

## 비용·삭제 가드

- 이 모듈은 Kafka/MSK, NAT Gateway, ECS/EKS 같은 상시 실행 컴퓨트를 만들지 않습니다.
- S3 `bronze/`와 `errors/` 객체, CloudWatch 로그는 기본 7일 후 정리됩니다.
- `force_destroy = false`가 기본이라 bucket에 객체가 남아 있으면 실수로 삭제되지 않습니다.
- 실습 종료 시 Vector와 Docker를 먼저 내리고, bucket을 비운 뒤 `terraform destroy`를
  실행합니다. `force_destroy = true`는 데이터 삭제를 명시적으로 감수할 때만 사용합니다.

참고: [Firehose pricing](https://aws.amazon.com/firehose/pricing/),
[S3 pricing](https://aws.amazon.com/s3/pricing/),
[Athena pricing](https://aws.amazon.com/athena/pricing/).
