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
  printf 'maxio-it-cond-%s-%s' "$(date +%s)" "$RANDOM"
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

with_bucket_and_object() {
  local bucket object_file

  bucket="$(create_bucket_name)"
  object_file="$SCRIPT_DIR/.tmp-conditional-src-$RANDOM.txt"
  printf 'conditional-content' >"$object_file"

  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3 cp "$object_file" "s3://$bucket/conditional.txt" "${AWS_ARGS[@]}" >/dev/null

  trap 'rm -f "$object_file"; cleanup_bucket "$bucket"' RETURN
  "$1" "$bucket"
}

expect_aws_error_status() {
  local expected_status="$1"
  shift

  local err_file="$SCRIPT_DIR/.tmp-conditional-err-$RANDOM.log"

  set +e
  "$@" >/dev/null 2>"$err_file"
  local status=$?
  set -e

  local err_text=''
  if [ -f "$err_file" ]; then
    err_text="$(<"$err_file")"
  fi
  rm -f "$err_file"

  [ "$status" -ne 0 ] && [[ "$err_text" == *"($expected_status)"* ]]
}

test_get_object_if_match_matching_impl() {
  local bucket="$1"
  local etag out_file

  etag="$(aws s3api head-object --bucket "$bucket" --key 'conditional.txt' --query 'ETag' --output text "${AWS_ARGS[@]}")"
  out_file="$SCRIPT_DIR/.tmp-conditional-if-match-ok-$RANDOM.txt"

  aws s3api get-object --bucket "$bucket" --key 'conditional.txt' --if-match "$etag" "$out_file" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$out_file"
}

test_get_object_if_match_non_matching_impl() {
  local bucket="$1"
  local out_file

  out_file="$SCRIPT_DIR/.tmp-conditional-if-match-fail-$RANDOM.txt"
  expect_aws_error_status 'PreconditionFailed' aws s3api get-object --bucket "$bucket" --key 'conditional.txt' --if-match '"00000000000000000000000000000000"' "$out_file" "${AWS_ARGS[@]}"
  rm -f "$out_file"
}

test_get_object_if_none_match_matching_impl() {
  local bucket="$1"
  local etag out_file

  etag="$(aws s3api head-object --bucket "$bucket" --key 'conditional.txt' --query 'ETag' --output text "${AWS_ARGS[@]}")"
  out_file="$SCRIPT_DIR/.tmp-conditional-if-none-match-304-$RANDOM.txt"

  expect_aws_error_status '304' aws s3api get-object --bucket "$bucket" --key 'conditional.txt' --if-none-match "$etag" "$out_file" "${AWS_ARGS[@]}"
  rm -f "$out_file"
}

test_get_object_if_none_match_non_matching_impl() {
  local bucket="$1"
  local out_file

  out_file="$SCRIPT_DIR/.tmp-conditional-if-none-match-200-$RANDOM.txt"
  aws s3api get-object --bucket "$bucket" --key 'conditional.txt' --if-none-match '"11111111111111111111111111111111"' "$out_file" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$out_file"
}

test_get_object_if_modified_since_impl() {
  local bucket="$1"
  local out_file

  out_file="$SCRIPT_DIR/.tmp-conditional-if-modified-since-$RANDOM.txt"
  aws s3api get-object --bucket "$bucket" --key 'conditional.txt' --if-modified-since 'Mon, 01 Jan 1990 00:00:00 GMT' "$out_file" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$out_file"
}

test_get_object_if_unmodified_since_impl() {
  local bucket="$1"
  local out_file

  out_file="$SCRIPT_DIR/.tmp-conditional-if-unmodified-since-$RANDOM.txt"
  expect_aws_error_status 'PreconditionFailed' aws s3api get-object --bucket "$bucket" --key 'conditional.txt' --if-unmodified-since 'Mon, 01 Jan 1990 00:00:00 GMT' "$out_file" "${AWS_ARGS[@]}"
  rm -f "$out_file"
}

test_head_object_conditional_headers_impl() {
  local bucket="$1"
  local etag

  etag="$(aws s3api head-object --bucket "$bucket" --key 'conditional.txt' --query 'ETag' --output text "${AWS_ARGS[@]}")"

  aws s3api head-object --bucket "$bucket" --key 'conditional.txt' --if-match "$etag" "${AWS_ARGS[@]}" >/dev/null
  expect_aws_error_status '304' aws s3api head-object --bucket "$bucket" --key 'conditional.txt' --if-none-match "$etag" "${AWS_ARGS[@]}"
}

test_get_object_if_match_matching() { with_bucket_and_object test_get_object_if_match_matching_impl; }
test_get_object_if_match_non_matching() { with_bucket_and_object test_get_object_if_match_non_matching_impl; }
test_get_object_if_none_match_matching() { with_bucket_and_object test_get_object_if_none_match_matching_impl; }
test_get_object_if_none_match_non_matching() { with_bucket_and_object test_get_object_if_none_match_non_matching_impl; }
test_get_object_if_modified_since() { with_bucket_and_object test_get_object_if_modified_since_impl; }
test_get_object_if_unmodified_since() { with_bucket_and_object test_get_object_if_unmodified_since_impl; }
test_head_object_conditional_headers() { with_bucket_and_object test_head_object_conditional_headers_impl; }

main() {
  printf 'Running conditional request integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'GET object with If-Match (matching ETag) returns 200' test_get_object_if_match_matching
  run_test 'GET object with If-Match (non-matching ETag) returns 412' test_get_object_if_match_non_matching
  run_test 'GET object with If-None-Match (matching ETag) returns 304' test_get_object_if_none_match_matching
  run_test 'GET object with If-None-Match (non-matching ETag) returns 200' test_get_object_if_none_match_non_matching
  run_test 'GET object with If-Modified-Since' test_get_object_if_modified_since
  run_test 'GET object with If-Unmodified-Since' test_get_object_if_unmodified_since
  run_test 'HEAD object with conditional headers' test_head_object_conditional_headers

  printf 'Conditional request tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
