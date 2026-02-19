#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-http://localhost:9000}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
AWS_EC2_METADATA_DISABLED="true"
AWS_MAX_ATTEMPTS="${AWS_MAX_ATTEMPTS:-2}"
AWS_RETRY_MODE="${AWS_RETRY_MODE:-standard}"
AWS_CLI_CONNECT_TIMEOUT="${AWS_CLI_CONNECT_TIMEOUT:-3}"
AWS_CLI_READ_TIMEOUT="${AWS_CLI_READ_TIMEOUT:-5}"

export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION AWS_EC2_METADATA_DISABLED
export AWS_MAX_ATTEMPTS AWS_RETRY_MODE AWS_CLI_CONNECT_TIMEOUT AWS_CLI_READ_TIMEOUT

AWS_ARGS=(--endpoint-url "$S3_ENDPOINT_URL")

PASS_COUNT=0
FAIL_COUNT=0

create_bucket_name() {
  printf 'maxio-it-encryption-%s-%s' "$(date +%s)" "$RANDOM"
}

cleanup_bucket() {
  local bucket="$1"
  aws s3 rm "s3://$bucket" --recursive "${AWS_ARGS[@]}" >/dev/null 2>&1 || true
  aws s3 rb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null 2>&1 || true
}

run_test() {
  local name="$1"
  local fn="$2"
  local status

  printf '[RUN ] %s\n' "$name"
  set +e
  ( set -e; "$fn" )
  status=$?
  set -e

  if [ "$status" -eq 0 ]; then
    PASS_COUNT=$((PASS_COUNT + 1))
    printf '[PASS] %s\n' "$name"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    printf '[FAIL] %s\n' "$name"
  fi
}

with_bucket() {
  local bucket
  bucket="$(create_bucket_name)"
  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  trap 'cleanup_bucket "$bucket"' RETURN
  "$1" "$bucket"
}

write_encryption_config() {
  local file="$1"
  cat >"$file" <<EOF
{
  "Rules": [
    {
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      }
    }
  ]
}
EOF
}

test_put_bucket_encryption_impl() {
  local bucket="$1"
  local encryption_json="$SCRIPT_DIR/.tmp-encryption-put-$RANDOM.json"

  write_encryption_config "$encryption_json"
  aws s3api put-bucket-encryption --bucket "$bucket" --server-side-encryption-configuration "file://$encryption_json" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$encryption_json"
}

test_get_bucket_encryption_impl() {
  local bucket="$1"
  local encryption_json="$SCRIPT_DIR/.tmp-encryption-get-$RANDOM.json"
  local algorithm

  write_encryption_config "$encryption_json"
  aws s3api put-bucket-encryption --bucket "$bucket" --server-side-encryption-configuration "file://$encryption_json" "${AWS_ARGS[@]}" >/dev/null
  algorithm="$(aws s3api get-bucket-encryption --bucket "$bucket" --query 'ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm' --output text "${AWS_ARGS[@]}")"

  rm -f "$encryption_json"
  [ "$algorithm" = 'AES256' ]
}

test_delete_bucket_encryption_impl() {
  local bucket="$1"
  local encryption_json="$SCRIPT_DIR/.tmp-encryption-del-$RANDOM.json"

  write_encryption_config "$encryption_json"
  aws s3api put-bucket-encryption --bucket "$bucket" --server-side-encryption-configuration "file://$encryption_json" "${AWS_ARGS[@]}" >/dev/null
  aws s3api delete-bucket-encryption --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null

  rm -f "$encryption_json"
  if aws s3api get-bucket-encryption --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
    return 1
  fi
}

test_get_bucket_encryption_not_found_impl() {
  local bucket="$1"
  local stderr_file="$SCRIPT_DIR/.tmp-encryption-missing-$RANDOM.stderr"

  if aws s3api get-bucket-encryption --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>"$stderr_file"; then
    rm -f "$stderr_file"
    return 1
  fi

  grep -q 'ServerSideEncryptionConfigurationNotFoundError' "$stderr_file"
  rm -f "$stderr_file"
}

test_put_bucket_encryption() { with_bucket test_put_bucket_encryption_impl; }
test_get_bucket_encryption() { with_bucket test_get_bucket_encryption_impl; }
test_delete_bucket_encryption() { with_bucket test_delete_bucket_encryption_impl; }
test_get_bucket_encryption_not_found() { with_bucket test_get_bucket_encryption_not_found_impl; }

main() {
  printf 'Running bucket encryption integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put bucket encryption (aws s3api put-bucket-encryption)' test_put_bucket_encryption
  run_test 'Get bucket encryption (aws s3api get-bucket-encryption)' test_get_bucket_encryption
  run_test 'Delete bucket encryption (aws s3api delete-bucket-encryption)' test_delete_bucket_encryption
  run_test 'Get missing encryption returns ServerSideEncryptionConfigurationNotFoundError (aws s3api get-bucket-encryption)' test_get_bucket_encryption_not_found

  printf 'Encryption tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
