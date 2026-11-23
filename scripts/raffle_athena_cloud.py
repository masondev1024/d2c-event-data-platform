"""Temporary Seoul AWS resources for the local raffle Spark / Athena comparison.

Host dependency: boto3. Pass a temporary operator boto3 Session (or use SSO).
After the parent communicates the price, call CloudRun.create(path,
price_notice_ack=True). Use one private, durable state path per run. Pass only
writer_env() to local Spark. Keep credentials out of argv, logs and files.
validate(snapshot_id, oracle_rows, observation=stage) returns a JSON-safe proof.
ValidationError.report contains failed query or comparison evidence. Pass the
complete MySQL oracle as an iterable of mappings.
Stop all local writers before cleanup(). If creation, validation or teardown
is interrupted, CloudRun.open(path).cleanup() resumes by exact stack identity.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
from tempfile import NamedTemporaryFile
import time
from uuid import uuid4

REGION = "ap-northeast-2"
OBSERVATIONS = frozenset({"initial", "sealed", "backfill", "retry", "historical"})
MAX_QUERIES = 20
BYTES_CUTOFF = 64 * 1024 * 1024
PROJECT = "raffle-10k"
COLUMNS = ("window_key", "product_id", "window_state", "entry_count")
BIGINT_MAX = 2**63 - 1
COMPARE_SQL = Path(__file__).resolve().parents[1] / "sql" / "raffle_athena_compare.sql"


class ValidationError(RuntimeError):
    def __init__(self, message, report):
        super().__init__(f"{message}: {json.dumps(report, sort_keys=True)}")
        self.report = report


def _require(ok, message):
    if not ok:
        raise RuntimeError(message)


def _tags(items):
    return {tag["Key"]: tag["Value"] for tag in items}


def _not_found_message(error):
    message = error["Message"].lower()
    return "does not exist" in message or "not found" in message


def _client(session, service):
    from botocore.config import Config

    return session.client(service, region_name=REGION, config=Config(
        connect_timeout=5, read_timeout=30,
        retries={"mode": "standard", "total_max_attempts": 3},
    ))


def _operator(session):
    if session is None:
        import boto3

        session = boto3.Session(region_name=REGION)
    credentials = session.get_credentials()
    _require(credentials and credentials.get_frozen_credentials().token,
             "supply a temporary operator session (for example AWS SSO)")
    return session


def _allow(actions, resources):
    return {"Effect": "Allow", "Action": actions, "Resource": resources}


def _bigint(value):
    return type(value) is int and -2**63 <= value <= BIGINT_MAX


def _oracle_index(rows):
    expected = {}
    for row in rows:
        if not isinstance(row, Mapping) or any(column not in row for column in COLUMNS):
            raise ValueError(f"oracle rows must supply {COLUMNS}")
        window, product, state, count = (row[column] for column in COLUMNS)
        if window not in ("A", "B") or not _bigint(product) or state not in ("OPEN", "CLOSED"):
            raise ValueError("invalid oracle window, product or state")
        if count is not None and (not _bigint(count) or count < 0):
            raise ValueError("oracle entry_count must be a nonnegative BIGINT or NULL")
        if (window, product) in expected:
            raise ValueError("duplicate product/window key in MySQL oracle")
        expected[window, product] = (state, count)
    if not expected or {p for w, p in expected if w == "A"} != {p for w, p in expected if w == "B"}:
        raise ValueError("oracle must contain both windows for every product")
    return expected


def _compare(expected, groups):
    multiplicity = Counter()
    state_mismatches = set()
    count_mismatches = set()
    for window, product, state, count, copies in groups:
        key = (window, product)
        multiplicity[key] += copies
        if key in expected:
            exp_state, exp_count = expected[key]
            if state != exp_state:
                state_mismatches.add(key)
            if count != exp_count:
                count_mismatches.add(key)
    checks = {
        "missing_keys": len(expected.keys() - multiplicity.keys()),
        "extra_keys": len(multiplicity.keys() - expected.keys()),
        "duplicate_keys": sum(n > 1 for n in multiplicity.values()),
        "duplicate_rows": sum(n - 1 for n in multiplicity.values()),
        "state_mismatches": len(state_mismatches),
        "count_mismatches": len(count_mismatches),
    }
    return {"expected_rows": len(expected), "actual_rows": sum(multiplicity.values()),
            "passed": not any(checks.values()), **checks}


def _result_groups(athena, query_id):
    """Athena sends the column-name row once, on the first SELECT result page."""
    names = [*COLUMNS, "copies"]
    types = ["varchar", "bigint", "varchar", "bigint", "bigint"]
    pages = athena.get_paginator("get_query_results").paginate(
        QueryExecutionId=query_id, PaginationConfig={"PageSize": 1000})
    header_seen = False
    for page in pages:
        result = page["ResultSet"]
        schema = result["ResultSetMetadata"]["ColumnInfo"]
        _require([c["Name"] for c in schema] == names and [c["Type"] for c in schema] == types,
                 f"unexpected Athena result schema: {[(c['Name'], c['Type']) for c in schema]}")
        for row in result["Rows"]:
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            if not header_seen:
                _require(values == names, "missing Athena SELECT header")
                header_seen = True
                continue
            _require(len(values) == len(names), "invalid Athena result width")
            window, product, state, count, copies = values
            copies = int(copies)
            _require(copies > 0, "invalid grouped row multiplicity")
            yield (window, None if product is None else int(product), state,
                   None if count is None else int(count), copies)
    _require(header_seen, "missing Athena SELECT result")


class CloudRun:
    def __init__(self, path, session, state):
        self.state_path = Path(path).absolute()
        self.session = session
        self._state = state
        self.run_id = state["run_id"]
        self.account = state["account"]
        _require(re.fullmatch("[a-f0-9]{32}", self.run_id)
                 and re.fullmatch("[0-9]{12}", self.account), "invalid run identity")
        self.stack_name = f"{PROJECT}-{self.run_id}"
        self.bucket = f"{PROJECT}-{self.account}-{self.run_id}"
        self.database = self.stack_name.replace("-", "_")
        self.workgroup = self.stack_name
        self.warehouse_uri = f"s3://{self.bucket}/warehouse"
        self.results_uri = f"s3://{self.bucket}/results/"

    @classmethod
    def create(cls, state_path, *, price_notice_ack=False, session=None):
        from botocore.exceptions import ClientError

        if price_notice_ack is not True:
            raise ValueError("parent price notice must be acknowledged before provisioning")
        session = _operator(session)
        caller = _client(session, "sts").get_caller_identity()
        run = cls(state_path, session, {"run_id": uuid4().hex, "account": caller["Account"],
                                       "phase": "creating", "queries": []})
        run.state_path.parent.mkdir(parents=True, exist_ok=True)
        with run._lock(initial=True):
            run._save()
            cfn = _client(session, "cloudformation")
            try:
                created = cfn.create_stack(
                    StackName=run.stack_name, ClientRequestToken=run.run_id,
                    Capabilities=["CAPABILITY_NAMED_IAM"], OnFailure="DO_NOTHING",
                    TimeoutInMinutes=10, TemplateBody=json.dumps(run._template(caller["UserId"])),
                    Tags=run._ownership_tags(),
                )
            except ClientError as exc:
                if exc.response["Error"]["Code"] in {
                    "AccessDenied", "AccessDeniedException", "ValidationError", "InsufficientCapabilitiesException"
                }:
                    run._state["phase"] = "create_rejected"
                    run._save()
                raise
            run._state["stack_id"] = created["StackId"]
            run._save()
            stack = run._wait_stack()
            _require(stack["StackStatus"] == "CREATE_COMPLETE",
                     f"provisioning ended in {stack['StackStatus']}; recover with open(path).cleanup()")
            run._state["phase"] = "ready"
            run._save()
        return run

    @classmethod
    def open(cls, state_path, *, session=None):
        state = json.loads(Path(state_path).read_text())
        return cls(state_path, _operator(session), state)

    @contextmanager
    def _lock(self, *, initial=False):
        # The lock inode is stable while the journal is atomically replaced.
        with self.state_path.with_name(self.state_path.name + ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if initial:
                if self.state_path.exists():
                    raise FileExistsError(self.state_path)
            else:
                state = json.loads(self.state_path.read_text())
                _require((state["run_id"], state["account"]) == (self.run_id, self.account),
                         "run journal identity changed")
                self._state = state
            yield

    def _save(self):
        directory = self.state_path.parent
        with NamedTemporaryFile(mode="w", dir=directory, delete=False) as tmp:
            try:
                json.dump(self._state, tmp, sort_keys=True)
                tmp.flush()
                os.fsync(tmp.fileno())
                os.replace(tmp.name, self.state_path)
            finally:
                Path(tmp.name).unlink(missing_ok=True)
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _ownership_tags(self):
        return [{"Key": "Project", "Value": PROJECT}, {"Key": "RaffleRun", "Value": self.run_id}]

    def _template(self, caller_id):
        bucket = f"arn:aws:s3:::{self.bucket}"
        glue = f"arn:aws:glue:{REGION}:{self.account}"
        athena = f"arn:aws:athena:{REGION}:{self.account}"
        common = [f"{glue}:catalog", f"{glue}:database/{self.database}"]
        tags = self._ownership_tags() + [{"Key": "RaffleStack", "Value": {"Ref": "AWS::StackId"}}]
        trust = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{self.account}:root"},
            "Action": "sts:AssumeRole", "Condition": {"StringEquals": {"aws:userid": caller_id}},
        }]}

        def role(kind, statements):
            return {"Type": "AWS::IAM::Role", "Properties": {
                "RoleName": f"{self.stack_name}-{kind}", "AssumeRolePolicyDocument": trust,
                "MaxSessionDuration": 3600, "Tags": tags,
                "Policies": [{"PolicyName": kind, "PolicyDocument": {
                    "Version": "2012-10-17", "Statement": statements}}],
            }}

        data_read = _allow(["s3:GetObject"], f"{bucket}/warehouse/*")
        writable = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        location = _allow(["s3:GetBucketLocation"], bucket)
        reader = role("reader", [
            location, _allow(["s3:ListBucket", "s3:ListBucketMultipartUploads"], bucket), data_read,
            _allow(["s3:GetObject", *writable], f"{bucket}/results/*"),
            _allow(["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
                   [*common, f"{glue}:table/{self.database}/raffle_mart"]),
            _allow(["athena:GetDataCatalog"], f"{athena}:datacatalog/AwsDataCatalog"),
            _allow(["athena:GetWorkGroup", "athena:StartQueryExecution", "athena:GetQueryExecution",
                    "athena:GetQueryResults", "athena:StopQueryExecution"],
                   f"{athena}:workgroup/{self.workgroup}"),
        ])
        writer = role("writer", [
            # Iceberg probes new objects with HeadObject before create(); S3
            # needs bucket-level ListBucket to return 404 for an absent key.
            location, _allow(["s3:ListBucket"], bucket),
            _allow(["s3:GetObject", "s3:DeleteObject", *writable], f"{bucket}/warehouse/*"),
            _allow(["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
                    "glue:UpdateTable"], [*common, f"{glue}:table/{self.database}/raffle_mart"]),
        ])
        return {"Resources": {
            "Bucket": {"Type": "AWS::S3::Bucket", "Properties": {
                "BucketName": self.bucket, "Tags": tags, "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True, "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True, "RestrictPublicBuckets": True},
                "BucketEncryption": {"ServerSideEncryptionConfiguration": [{
                    "ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
            }},
            "Database": {"Type": "AWS::Glue::Database", "Properties": {
                "CatalogId": self.account, "DatabaseInput": {
                    "Name": self.database, "LocationUri": self.warehouse_uri + "/",
                    "Parameters": {"raffle_stack": {"Ref": "AWS::StackId"}, "raffle_run": self.run_id},
                }}},
            "Workgroup": {"Type": "AWS::Athena::WorkGroup", "DependsOn": "Bucket", "Properties": {
                "Name": self.workgroup, "RecursiveDeleteOption": True, "Tags": tags,
                "WorkGroupConfiguration": {
                    "EnforceWorkGroupConfiguration": True, "BytesScannedCutoffPerQuery": BYTES_CUTOFF,
                    "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
                    "ResultConfiguration": {"OutputLocation": self.results_uri,
                        "ExpectedBucketOwner": self.account,
                        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}},
                }}},
            "Reader": reader, "Writer": writer,
        }}

    def _stack(self, *, allow_missing=False):
        from botocore.exceptions import ClientError

        cfn = _client(self.session, "cloudformation")
        target = self._state.get("stack_id", self.stack_name)
        try:
            stack, = cfn.describe_stacks(StackName=target)["Stacks"]
        except ClientError as exc:
            error = exc.response["Error"]
            if allow_missing and error["Code"] == "ValidationError" and "does not exist" in error["Message"]:
                return None
            raise
        prefix = f"arn:aws:cloudformation:{REGION}:{self.account}:stack/{self.stack_name}/"
        tags = _tags(stack.get("Tags", []))
        _require(stack["StackName"] == self.stack_name and stack["StackId"].startswith(prefix)
                 and tags.get("Project") == PROJECT and tags.get("RaffleRun") == self.run_id
                 and stack["StackId"] == self._state.get("stack_id", stack["StackId"]),
                 "CloudFormation ownership mismatch")
        if "stack_id" not in self._state:
            # Reconcile acceptance of a create whose response was lost.
            self._state["stack_id"] = stack["StackId"]
            self._save()
        return stack

    def _wait_stack(self, *, deleting=False):
        for _ in range(150):
            stack = self._stack(allow_missing=True)
            if stack is None:
                if deleting:
                    return {"StackStatus": "DELETE_COMPLETE"}
            elif deleting:
                if stack["StackStatus"] in ("DELETE_COMPLETE", "DELETE_FAILED"):
                    return stack
            elif not stack["StackStatus"].endswith("_IN_PROGRESS"):
                return stack
            time.sleep(5)
        raise TimeoutError("stack operation still pending; resume cleanup using the same journal")

    def _assume(self, kind):
        from botocore.exceptions import ClientError

        name = f"{self.stack_name}-{kind}"
        sts = _client(self.session, "sts")
        for attempt in range(6):
            try:
                response = sts.assume_role(RoleArn=f"arn:aws:iam::{self.account}:role/{name}",
                                           RoleSessionName=f"raffle-{kind}", DurationSeconds=3600)
                return response["Credentials"], response["AssumedRoleUser"]["Arn"]
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "AccessDenied" or attempt == 5:
                    raise
                time.sleep(2**attempt)  # Newly created IAM roles may take time to propagate.

    def _ready(self):
        _require(self._state["phase"] == "ready" and self._stack()["StackStatus"] == "CREATE_COMPLETE",
                 "run is not ready")

    def writer_env(self):
        with self._lock():
            self._ready()
            creds, _ = self._assume("writer")
            return {"AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
                    "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
                    "AWS_SESSION_TOKEN": creds["SessionToken"],
                    "AWS_REGION": REGION, "AWS_DEFAULT_REGION": REGION}

    def _reader(self):
        import boto3

        creds, arn = self._assume("reader")
        session = boto3.Session(aws_access_key_id=creds["AccessKeyId"],
                                aws_secret_access_key=creds["SecretAccessKey"],
                                aws_session_token=creds["SessionToken"], region_name=REGION)
        return _client(session, "athena"), arn

    def _check_workgroup(self, athena):
        wg = athena.get_work_group(WorkGroup=self.workgroup)["WorkGroup"]
        config = wg["Configuration"]
        result = config["ResultConfiguration"]
        _require(wg["Name"] == self.workgroup and wg["State"] == "ENABLED"
                 and config["EnforceWorkGroupConfiguration"] is True
                 and config["BytesScannedCutoffPerQuery"] == BYTES_CUTOFF
                 and config["EngineVersion"]["SelectedEngineVersion"] == "Athena engine version 3"
                 and result["OutputLocation"] == self.results_uri
                 and result["ExpectedBucketOwner"] == self.account
                 and result["EncryptionConfiguration"]["EncryptionOption"] == "SSE_S3",
                 "Athena workgroup configuration drift")

    def _query_end(self, athena, query_id, *, polls):
        for _ in range(polls):
            execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
            _require(execution["QueryExecutionId"] == query_id and execution["WorkGroup"] == self.workgroup,
                     "unexpected query identity or workgroup")
            if execution["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                return execution
            time.sleep(1)
        return None

    def validate(self, snapshot_id, oracle_rows: Iterable[Mapping], *, observation: str):
        if observation not in OBSERVATIONS:
            raise ValueError(f"observation must be one of {sorted(OBSERVATIONS)}")
        if not _bigint(snapshot_id) or snapshot_id <= 0:
            raise ValueError("snapshot_id must be a positive BIGINT")
        expected = _oracle_index(oracle_rows)
        sql = COMPARE_SQL.read_text().strip().format(snapshot_id=snapshot_id)
        with self._lock():
            self._ready()
            athena, reader_arn = self._reader()
            self._check_workgroup(athena)
            queries = self._state["queries"]
            # Reuse the same token even across an ambiguous StartQueryExecution response.
            # A terminally failed validation is evidence; it does not trigger a paid retry.
            digest = hashlib.sha256(sql.strip().encode()).hexdigest()
            slot = next((q for q in queries
                         if q["observation"] == observation and q["digest"] == digest), None)
            if slot is None:
                _require(len(queries) < MAX_QUERIES, "20-query run budget exhausted")
                slot = {"observation": observation, "digest": digest, "snapshot_id": snapshot_id,
                        "token": f"{self.run_id}-{len(queries) + 1:02d}"}
                queries.append(slot)
                self._save()
            if "query_id" not in slot:
                response = athena.start_query_execution(
                    QueryString=sql, ClientRequestToken=slot["token"], WorkGroup=self.workgroup,
                    QueryExecutionContext={"Catalog": "AwsDataCatalog", "Database": self.database},
                    ResultReuseConfiguration={"ResultReuseByAgeConfiguration": {"Enabled": False}},
                )
                new_id = response["QueryExecutionId"]
                _require(all(q.get("query_id") != new_id for q in queries),
                         "distinct Athena observations returned the same execution ID")
                slot["query_id"] = new_id
                self._save()
            query_id = slot["query_id"]
            execution = self._query_end(athena, query_id, polls=120)
            if execution is None:
                athena.stop_query_execution(QueryExecutionId=query_id)
                execution = self._query_end(athena, query_id, polls=30)
                _require(execution, f"query {query_id} cancellation still pending")
            status = execution["Status"]
            stats = execution.get("Statistics", {})
            report = {"observation": observation, "snapshot_id": snapshot_id, "query_id": query_id,
                      "reader_arn": reader_arn, "workgroup": self.workgroup, "database": self.database, "region": REGION,
                      "state": status["State"], "bytes_scanned": stats.get("DataScannedInBytes"),
                      "duration_ms": stats.get("TotalExecutionTimeInMillis"),
                      "queries_reserved": len(queries), "bytes_cutoff": BYTES_CUTOFF}
            slot["execution"] = {key: report[key] for key in ("state", "bytes_scanned", "duration_ms")}
            self._save()
            if status["State"] != "SUCCEEDED":
                report["reason"] = status.get("StateChangeReason")
                raise ValidationError("Athena execution failed", report)
            context = execution["QueryExecutionContext"]
            _require(execution["Query"].strip() == sql
                     and context.get("Catalog", "").casefold() == "awsdatacatalog"
                     and context.get("Database") == self.database
                     and not stats.get("ResultReuseInformation", {}).get("ReusedPreviousResult", False),
                     "Athena execution does not match pinned query")
            if not (type(report["bytes_scanned"]) is int and 0 <= report["bytes_scanned"] <= BYTES_CUTOFF
                    and type(report["duration_ms"]) is int and report["duration_ms"] >= 0):
                raise ValidationError("invalid Athena execution statistics", report)
            report.update(_compare(expected, _result_groups(athena, query_id)))
            if not report["passed"]:
                raise ValidationError("Athena differs from MySQL oracle", report)
            return report

    def _check_tags(self, items):
        tags = _tags(items)
        _require(tags.get("RaffleRun") == self.run_id and tags.get("Project") == PROJECT
                 and tags.get("RaffleStack") == self._state["stack_id"],
                 "resource ownership tags do not match the creating stack")

    def _bucket_guard(self, s3):
        owner = {"Bucket": self.bucket, "ExpectedBucketOwner": self.account}
        self._check_tags(s3.get_bucket_tagging(**owner)["TagSet"])
        _require(s3.get_bucket_location(**owner)["LocationConstraint"] == REGION,
                 "unexpected S3 bucket region")
        _require(not s3.get_bucket_versioning(**owner).get("Status"),
                 "unexpected versioned bucket; refusing object deletion")
        return owner

    def _db_guard(self):
        glue = _client(self.session, "glue")
        db = glue.get_database(CatalogId=self.account, Name=self.database)["Database"]
        params = db.get("Parameters", {})
        _require(params.get("raffle_stack") == self._state["stack_id"]
                 and params.get("raffle_run") == self.run_id
                 and db["LocationUri"].rstrip("/") == self.warehouse_uri,
                 "Glue database ownership mismatch")
        pages = glue.get_paginator("get_tables").paginate(CatalogId=self.account, DatabaseName=self.database)
        for page in pages:
            for table in page["TableList"]:
                location = table.get("StorageDescriptor", {}).get("Location", "")
                metadata = table.get("Parameters", {}).get("metadata_location", "")
                _require(table["Name"] == "raffle_mart"
                         and location.startswith(self.warehouse_uri + "/")
                         and metadata.startswith(self.warehouse_uri + "/"),
                         f"refusing to cascade-delete unexpected Glue table {table['Name']}")

    def _guard_resources(self):
        """CloudFormation owns lifecycle; independently verify live ownership before deletion."""
        from botocore.exceptions import ClientError

        expected = {"Bucket": ("AWS::S3::Bucket", self.bucket),
                    "Database": ("AWS::Glue::Database", self.database),
                    "Workgroup": ("AWS::Athena::WorkGroup", self.workgroup),
                    "Writer": ("AWS::IAM::Role", self.stack_name + "-writer"),
                    "Reader": ("AWS::IAM::Role", self.stack_name + "-reader")}
        missing = {"Bucket": {"NoSuchBucket"}, "Database": {"EntityNotFoundException"},
                   "Workgroup": {"InvalidRequestException", "ResourceNotFoundException"},
                   "Writer": {"NoSuchEntity"}, "Reader": {"NoSuchEntity"}}
        cfn = _client(self.session, "cloudformation")
        pages = cfn.get_paginator("list_stack_resources").paginate(StackName=self._state["stack_id"])
        live = set()
        for page in pages:
            for resource in page["StackResourceSummaries"]:
                key = resource["LogicalResourceId"]
                _require(key in expected and expected[key][0] == resource["ResourceType"],
                         "unexpected resource in stack")
                if resource["ResourceStatus"] == "DELETE_COMPLETE" or not resource.get("PhysicalResourceId"):
                    continue
                name = resource["PhysicalResourceId"]
                _require(name == expected[key][1], f"unexpected {key} physical identity")
                try:
                    if key == "Bucket":
                        self._bucket_guard(_client(self.session, "s3"))
                    elif key == "Workgroup":
                        athena = _client(self.session, "athena")
                        arn = f"arn:aws:athena:{REGION}:{self.account}:workgroup/{self.workgroup}"
                        self._check_tags(athena.list_tags_for_resource(ResourceARN=arn)["Tags"])
                    elif key == "Database":
                        self._db_guard()
                    else:
                        role = _client(self.session, "iam").get_role(RoleName=name)["Role"]
                        self._check_tags(role["Tags"])
                except ClientError as exc:
                    error = exc.response["Error"]
                    if error["Code"] not in missing[key]:
                        raise
                    if error["Code"] == "InvalidRequestException" and not _not_found_message(error):
                        raise
                    continue
                live.add(key)
        return live

    def _stop_queries(self):
        athena = _client(self.session, "athena")
        # A submission whose response was lost is discoverable in this unique workgroup.
        response = athena.list_query_executions(WorkGroup=self.workgroup, MaxResults=50)
        ids = set(response["QueryExecutionIds"]) | {
            slot["query_id"] for slot in self._state["queries"] if "query_id" in slot}
        _require(not response.get("NextToken") and len(ids) <= MAX_QUERIES,
                 "unexpected query volume in owned workgroup")
        reserved = Counter(slot["digest"] for slot in self._state["queries"])
        active = []
        for query_id in ids:
            execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
            _require(execution["WorkGroup"] == self.workgroup and execution["QueryExecutionId"] == query_id,
                     "refusing to stop a foreign query")
            digest = hashlib.sha256(execution["Query"].strip().encode()).hexdigest()
            _require(reserved[digest] > 0 and execution["QueryExecutionContext"].get("Database") == self.database,
                     "unexpected Athena query in run workgroup")
            reserved[digest] -= 1
            if execution["Status"]["State"] in ("QUEUED", "RUNNING"):
                active.append(query_id)
        for query_id in active:
            athena.stop_query_execution(QueryExecutionId=query_id)
            _require(self._query_end(athena, query_id, polls=30),
                     f"query {query_id} still active; resume cleanup")

    def _empty_bucket(self):
        from botocore.exceptions import ClientError

        s3 = _client(self.session, "s3")
        owner = self._bucket_guard(s3)
        prefixes = ("warehouse/", "results/")
        # Always fetch the first remaining page after mutation. Partial failures
        # are inspected and retried by relisting, without stale pagination tokens.
        for _ in range(100):
            uploads = s3.list_multipart_uploads(**owner).get("Uploads", [])
            if not uploads:
                break
            _require(all(u["Key"].startswith(prefixes) for u in uploads),
                     "multipart upload outside run prefixes")
            self._bucket_guard(s3)
            for upload in uploads:
                try:
                    s3.abort_multipart_upload(**owner, Key=upload["Key"], UploadId=upload["UploadId"])
                except ClientError as exc:
                    if exc.response["Error"]["Code"] != "NoSuchUpload":
                        raise
        else:
            raise RuntimeError("multipart cleanup batch limit reached; resume cleanup")
        consecutive_failures = 0
        for _ in range(100):
            objects = s3.list_objects_v2(**owner, MaxKeys=1000).get("Contents", [])
            if not objects:
                return
            _require(all(o["Key"].startswith(prefixes) for o in objects),
                     "S3 object outside run prefixes")
            self._bucket_guard(s3)
            response = s3.delete_objects(**owner, Delete={
                "Objects": [{"Key": o["Key"]} for o in objects], "Quiet": True})
            errors = response.get("Errors", [])
            if errors:
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    codes = sorted({e["Code"] for e in errors})
                    raise RuntimeError(f"S3 object deletes failed: {codes}; resume cleanup")
                time.sleep(consecutive_failures)
            else:
                consecutive_failures = 0
        raise RuntimeError("object cleanup batch limit reached; resume cleanup")

    def _verify_absent(self):
        from botocore.exceptions import ClientError

        checks = {
            "bucket": (_client(self.session, "s3").head_bucket,
                       {"Bucket": self.bucket, "ExpectedBucketOwner": self.account}, {"404", "NoSuchBucket"}),
            "database": (_client(self.session, "glue").get_database,
                         {"CatalogId": self.account, "Name": self.database}, {"EntityNotFoundException"}),
            "workgroup": (_client(self.session, "athena").get_work_group,
                          {"WorkGroup": self.workgroup}, {"ResourceNotFoundException", "InvalidRequestException"}),
        }
        iam = _client(self.session, "iam")
        for kind in ("reader", "writer"):
            checks[kind] = (iam.get_role, {"RoleName": f"{self.stack_name}-{kind}"}, {"NoSuchEntity"})
        remaining = dict(checks)
        for attempt in range(6):
            for name, (get_resource, params, not_found) in list(remaining.items()):
                try:
                    get_resource(**params)
                except ClientError as exc:
                    error = exc.response["Error"]
                    if error["Code"] not in not_found:
                        raise
                    if error["Code"] == "InvalidRequestException" and not _not_found_message(error):
                        raise
                    del remaining[name]
            if not remaining:
                return sorted(checks)
            if attempt < 5:
                time.sleep(2)
        raise RuntimeError(f"resource absence unconfirmed: {sorted(remaining)}; resume cleanup")

    def cleanup(self):
        """Stop local Spark first. A successful return records confirmed stack deletion."""
        with self._lock():
            if self._state["phase"] == "deleted":
                return self._state["teardown"]
            caller = _client(self.session, "sts").get_caller_identity()
            _require(caller["Account"] == self.account, "cleanup operator belongs to another account")
            stack = self._stack(allow_missing=True)
            if stack is None and self._state["phase"] == "creating":
                for delay in (1, 2, 4):
                    time.sleep(delay)
                    stack = self._stack(allow_missing=True)
                    if stack:
                        break
            if stack is None:
                # An indeterminate create must not be reported as a successful deletion.
                _require(self._state["phase"] in ("deleting", "create_rejected"),
                         "create outcome unknown and stack not found; retain journal and recheck")
            else:
                if stack["StackStatus"].endswith("_IN_PROGRESS"):
                    stack = self._wait_stack(deleting=stack["StackStatus"] == "DELETE_IN_PROGRESS")
                if stack["StackStatus"] != "DELETE_COMPLETE":
                    _require(stack["StackStatus"] in ("CREATE_COMPLETE", "CREATE_FAILED",
                             "ROLLBACK_COMPLETE", "ROLLBACK_FAILED", "DELETE_FAILED"),
                             f"unexpected stack status {stack['StackStatus']}")
                    self._state["phase"] = "deleting"
                    self._save()
                    live = self._guard_resources()
                    if "Workgroup" in live:
                        self._stop_queries()
                    if "Bucket" in live:
                        self._empty_bucket()
                    self._guard_resources()
                    cfn = _client(self.session, "cloudformation")
                    cfn.delete_stack(StackName=self._stack()["StackId"])
                    stack = self._wait_stack(deleting=True)
                    _require(stack["StackStatus"] == "DELETE_COMPLETE",
                             f"cleanup ended in {stack['StackStatus']}; retain journal and retry")
            absent = self._verify_absent() if "stack_id" in self._state else []
            result = {"status": "deleted", "stack_id": self._state.get("stack_id"), "verified_absent": absent}
            self._state.update(phase="deleted", teardown=result)
            self._save()
            return result
