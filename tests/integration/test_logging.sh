#!/usr/bin/env bash
set -e

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
  printf 'maxio-it-logging-%s-%s' "$(date +%s)" "$RANDOM"
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

with_source_and_target_bucket() {
  local source_bucket
  local target_bucket

  source_bucket="$(create_bucket_name)-src"
  target_bucket="$(create_bucket_name)-dst"

  aws s3 mb "s3://$source_bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3 mb "s3://$target_bucket" "${AWS_ARGS[@]}" >/dev/null

  trap 'cleanup_bucket "$source_bucket"; cleanup_bucket "$target_bucket"' RETURN
  "$1" "$source_bucket" "$target_bucket"
}

test_put_bucket_logging_impl() {
  local source_bucket="$1"
  local target_bucket="$2"

  aws s3api put-bucket-logging \
    --bucket "$source_bucket" \
    --bucket-logging-status "{\"LoggingEnabled\":{\"TargetBucket\":\"$target_bucket\",\"TargetPrefix\":\"logs/\"}}" \
    "${AWS_ARGS[@]}" >/dev/null
}

test_get_bucket_logging_impl() {
  local source_bucket="$1"
  local target_bucket="$2"
  local configured_target
  local configured_prefix

  aws s3api put-bucket-logging \
    --bucket "$source_bucket" \
    --bucket-logging-status "{\"LoggingEnabled\":{\"TargetBucket\":\"$target_bucket\",\"TargetPrefix\":\"logs/\"}}" \
    "${AWS_ARGS[@]}" >/dev/null

  configured_target="$(aws s3api get-bucket-logging --bucket "$source_bucket" --query 'LoggingEnabled.TargetBucket' --output text "${AWS_ARGS[@]}")"
  configured_prefix="$(aws s3api get-bucket-logging --bucket "$source_bucket" --query 'LoggingEnabled.TargetPrefix' --output text "${AWS_ARGS[@]}")"

  [ "$configured_target" = "$target_bucket" ] && [ "$configured_prefix" = 'logs/' ]
}

test_get_bucket_logging_empty_when_not_configured_impl() {
  local source_bucket="$1"
  local logging_enabled

  logging_enabled="$(aws s3api get-bucket-logging --bucket "$source_bucket" --query 'LoggingEnabled' --output text "${AWS_ARGS[@]}")"
  [ -z "$logging_enabled" ] || [ "$logging_enabled" = 'None' ]
}

test_put_bucket_logging() { with_source_and_target_bucket test_put_bucket_logging_impl; }
test_get_bucket_logging() { with_source_and_target_bucket test_get_bucket_logging_impl; }
test_get_bucket_logging_empty_when_not_configured() { with_source_and_target_bucket test_get_bucket_logging_empty_when_not_configured_impl; }

main() {
  printf 'Running logging integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put bucket logging configuration (aws s3api put-bucket-logging)' test_put_bucket_logging
  run_test 'Get bucket logging configuration (aws s3api get-bucket-logging)' test_get_bucket_logging
  run_test 'Get bucket logging returns empty LoggingEnabled when unset' test_get_bucket_logging_empty_when_not_configured

  printf 'Logging tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
