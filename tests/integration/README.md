# S3 Integration Tests (aws-cli)

This directory contains end-to-end S3 API integration tests for maxio using shell scripts and `aws-cli`.

## Test files

- `s3_test.sh`: Main test runner, executes all test suites and prints a summary.
- `test_buckets.sh`: Bucket operation tests.
- `test_objects.sh`: Object operation tests.
- `test_copy.sh`: CopyObject tests.
- `test_delete_objects.sh`: DeleteObjects tests.
- `test_acl.sh`: Bucket/Object ACL tests.
- `test_multipart.sh`: Multipart upload tests.
- `test_versioning.sh`: Versioning tests.
- `test_tagging.sh`: Object tagging tests.
- `test_lifecycle.sh`: Lifecycle configuration tests.

## Prerequisites

- `aws-cli` must be installed and available in `PATH`.
- maxio server must be running and reachable at `http://localhost:9000`.
- Test credentials:
  - `AWS_ACCESS_KEY_ID=minioadmin`
  - `AWS_SECRET_ACCESS_KEY=minioadmin`

## Configuration

All scripts support environment variable overrides:

- `S3_ENDPOINT_URL` (default: `http://localhost:9000`)
- `AWS_ACCESS_KEY_ID` (default: `minioadmin`)
- `AWS_SECRET_ACCESS_KEY` (default: `minioadmin`)
- `AWS_DEFAULT_REGION` (default: `us-east-1`)

The scripts always pass `--endpoint-url "$S3_ENDPOINT_URL"` to every `aws s3`/`aws s3api` call.

## Run all tests

```bash
./tests/integration/s3_test.sh
```

## Run a single suite

```bash
./tests/integration/test_objects.sh
```

## Covered API scenarios

### Bucket tests

- Create bucket (`aws s3 mb`)
- List buckets (`aws s3 ls`)
- Head bucket (`aws s3api head-bucket`)
- Delete bucket (`aws s3 rb`)
- Get bucket location (`aws s3api get-bucket-location`)

### Object tests

- Put/get/head/delete object
- List objects (v1 and v2)
- Put object with metadata
- Get object with range requests

### CopyObject tests

- Copy object within same bucket
- Copy object to a different bucket
- Copy metadata with `COPY` and `REPLACE` directives
- Conditional copy with `x-amz-copy-source-if-match`
- Source and copied object content validation

### DeleteObjects tests

- Delete multiple objects in one request
- Delete with `Quiet` mode
- Delete non-existent objects without errors
- Verify object removal with follow-up `head-object`

### ACL tests

- Get default bucket ACL
- Put bucket ACL with canned ACLs (`private`, `public-read`)
- Get object ACL
- Put object ACL
- Verify ACL owner/grant structure in responses

### Multipart tests

- Create multipart upload
- Upload part
- Complete multipart upload
- Abort multipart upload
- List multipart uploads
- Large file upload via `aws s3 cp` (multipart auto behavior)

### Versioning tests

- Enable and verify versioning status
- Upload multiple versions for one object key
- List object versions
- Get specific version
- Delete specific version

### Tagging tests

- Put object tagging
- Get object tagging
- Delete object tagging

### Lifecycle tests

- Put lifecycle configuration
- Get lifecycle configuration
- Delete lifecycle configuration

## Cleanup behavior

Each test suite uses unique bucket names and attempts cleanup after each test:

- Deletes objects and versions where applicable
- Deletes buckets
- Removes temporary local test files

If a test fails in the middle, rerun the suite or clean up remaining resources manually.
