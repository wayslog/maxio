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
  printf 'maxio-it-bucket-%s-%s' "$(date +%s)" "$RANDOM"
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

test_create_bucket() {
  local bucket
  bucket="$(create_bucket_name)"
  trap 'cleanup_bucket "$bucket"' RETURN

  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3api head-bucket --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null
}

test_list_buckets() {
  local bucket
  bucket="$(create_bucket_name)"
  trap 'cleanup_bucket "$bucket"' RETURN

  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3 ls "${AWS_ARGS[@]}" | grep -q "$bucket"
}

test_head_bucket() {
  local bucket
  bucket="$(create_bucket_name)"
  trap 'cleanup_bucket "$bucket"' RETURN

  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3api head-bucket --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null
}

test_delete_bucket() {
  local bucket
  bucket="$(create_bucket_name)"

  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3 rb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null

  if aws s3api head-bucket --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
    return 1
  fi
}

test_get_bucket_location() {
  local bucket location
  bucket="$(create_bucket_name)"
  trap 'cleanup_bucket "$bucket"' RETURN

  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  location="$(aws s3api get-bucket-location --bucket "$bucket" --query 'LocationConstraint' --output text "${AWS_ARGS[@]}")"

  if [ "$location" = "None" ] || [ "$location" = "null" ] || [ "$location" = "$AWS_DEFAULT_REGION" ]; then
    return 0
  fi

  return 1
}

main() {
  printf 'Running bucket integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Create bucket (aws s3 mb)' test_create_bucket
  run_test 'List buckets (aws s3 ls)' test_list_buckets
  run_test 'Head bucket (aws s3api head-bucket)' test_head_bucket
  run_test 'Delete bucket (aws s3 rb)' test_delete_bucket
  run_test 'Get bucket location (aws s3api get-bucket-location)' test_get_bucket_location

  printf 'Bucket tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
