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
  printf 'maxio-it-pab-%s-%s' "$(date +%s)" "$RANDOM"
}

cleanup_bucket() {
  local bucket="$1"
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

test_put_public_access_block_impl() {
  local bucket="$1"
  local block_policy

  aws s3api put-public-access-block \
    --bucket "$bucket" \
    --public-access-block-configuration 'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true' \
    "${AWS_ARGS[@]}" >/dev/null

  block_policy="$(aws s3api get-public-access-block --bucket "$bucket" --query 'PublicAccessBlockConfiguration.BlockPublicPolicy' --output text "${AWS_ARGS[@]}")"
  [ "$block_policy" = 'True' ]
}

test_get_public_access_block_impl() {
  local bucket="$1"
  local block_acls ignore_acls block_policy restrict_buckets

  aws s3api put-public-access-block \
    --bucket "$bucket" \
    --public-access-block-configuration 'BlockPublicAcls=true,IgnorePublicAcls=false,BlockPublicPolicy=true,RestrictPublicBuckets=false' \
    "${AWS_ARGS[@]}" >/dev/null

  block_acls="$(aws s3api get-public-access-block --bucket "$bucket" --query 'PublicAccessBlockConfiguration.BlockPublicAcls' --output text "${AWS_ARGS[@]}")"
  ignore_acls="$(aws s3api get-public-access-block --bucket "$bucket" --query 'PublicAccessBlockConfiguration.IgnorePublicAcls' --output text "${AWS_ARGS[@]}")"
  block_policy="$(aws s3api get-public-access-block --bucket "$bucket" --query 'PublicAccessBlockConfiguration.BlockPublicPolicy' --output text "${AWS_ARGS[@]}")"
  restrict_buckets="$(aws s3api get-public-access-block --bucket "$bucket" --query 'PublicAccessBlockConfiguration.RestrictPublicBuckets' --output text "${AWS_ARGS[@]}")"

  [ "$block_acls" = 'True' ] && [ "$ignore_acls" = 'False' ] && [ "$block_policy" = 'True' ] && [ "$restrict_buckets" = 'False' ]
}

test_delete_public_access_block_impl() {
  local bucket="$1"
  local err_file="$SCRIPT_DIR/.tmp-public-access-delete-$RANDOM.err"

  aws s3api put-public-access-block \
    --bucket "$bucket" \
    --public-access-block-configuration 'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true' \
    "${AWS_ARGS[@]}" >/dev/null

  aws s3api delete-public-access-block --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null

  set +e
  aws s3api get-public-access-block --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>"$err_file"
  local status=$?
  set -e

  local err_text=''
  if [ -f "$err_file" ]; then
    err_text="$(<"$err_file")"
  fi
  rm -f "$err_file"

  [ "$status" -ne 0 ] && [[ "$err_text" == *'NoSuchPublicAccessBlockConfiguration'* ]]
}

test_get_public_access_block_when_not_set_impl() {
  local bucket="$1"
  local err_file="$SCRIPT_DIR/.tmp-public-access-missing-$RANDOM.err"

  set +e
  aws s3api get-public-access-block --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>"$err_file"
  local status=$?
  set -e

  local err_text=''
  if [ -f "$err_file" ]; then
    err_text="$(<"$err_file")"
  fi
  rm -f "$err_file"

  [ "$status" -ne 0 ] && [[ "$err_text" == *'NoSuchPublicAccessBlockConfiguration'* ]]
}

test_put_public_access_block() { with_bucket test_put_public_access_block_impl; }
test_get_public_access_block() { with_bucket test_get_public_access_block_impl; }
test_delete_public_access_block() { with_bucket test_delete_public_access_block_impl; }
test_get_public_access_block_when_not_set() { with_bucket test_get_public_access_block_when_not_set_impl; }

main() {
  printf 'Running public access block integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put public access block configuration' test_put_public_access_block
  run_test 'Get public access block configuration' test_get_public_access_block
  run_test 'Delete public access block configuration' test_delete_public_access_block
  run_test 'Get public access block when not set returns NoSuchPublicAccessBlockConfiguration' test_get_public_access_block_when_not_set

  printf 'Public access block tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
