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
  printf 'maxio-it-owner-%s-%s' "$(date +%s)" "$RANDOM"
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

test_put_ownership_controls_impl() {
  local bucket="$1"
  local mode

  aws s3api put-bucket-ownership-controls \
    --bucket "$bucket" \
    --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerPreferred}]' \
    "${AWS_ARGS[@]}" >/dev/null

  mode="$(aws s3api get-bucket-ownership-controls --bucket "$bucket" --query 'OwnershipControls.Rules[0].ObjectOwnership' --output text "${AWS_ARGS[@]}")"
  [ "$mode" = 'BucketOwnerPreferred' ]
}

test_get_ownership_controls_impl() {
  local bucket="$1"
  local mode

  aws s3api put-bucket-ownership-controls \
    --bucket "$bucket" \
    --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]' \
    "${AWS_ARGS[@]}" >/dev/null

  mode="$(aws s3api get-bucket-ownership-controls --bucket "$bucket" --query 'OwnershipControls.Rules[0].ObjectOwnership' --output text "${AWS_ARGS[@]}")"
  [ "$mode" = 'BucketOwnerEnforced' ]
}

test_delete_ownership_controls_impl() {
  local bucket="$1"
  local err_file="$SCRIPT_DIR/.tmp-ownership-delete-$RANDOM.err"

  aws s3api put-bucket-ownership-controls \
    --bucket "$bucket" \
    --ownership-controls 'Rules=[{ObjectOwnership=ObjectWriter}]' \
    "${AWS_ARGS[@]}" >/dev/null

  aws s3api delete-bucket-ownership-controls --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null

  set +e
  aws s3api get-bucket-ownership-controls --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>"$err_file"
  local status=$?
  set -e

  local err_text=''
  if [ -f "$err_file" ]; then
    err_text="$(<"$err_file")"
  fi
  rm -f "$err_file"

  [ "$status" -ne 0 ] && [ -n "$err_text" ]
}

test_put_request_payment_impl() {
  local bucket="$1"
  local payer

  aws s3api put-bucket-request-payment \
    --bucket "$bucket" \
    --request-payment-configuration 'Payer=Requester' \
    "${AWS_ARGS[@]}" >/dev/null

  payer="$(aws s3api get-bucket-request-payment --bucket "$bucket" --query 'Payer' --output text "${AWS_ARGS[@]}")"
  [ "$payer" = 'Requester' ]
}

test_get_request_payment_impl() {
  local bucket="$1"
  local payer

  aws s3api put-bucket-request-payment \
    --bucket "$bucket" \
    --request-payment-configuration 'Payer=Requester' \
    "${AWS_ARGS[@]}" >/dev/null

  payer="$(aws s3api get-bucket-request-payment --bucket "$bucket" --query 'Payer' --output text "${AWS_ARGS[@]}")"
  [ "$payer" = 'Requester' ]
}

test_put_ownership_controls() { with_bucket test_put_ownership_controls_impl; }
test_get_ownership_controls() { with_bucket test_get_ownership_controls_impl; }
test_delete_ownership_controls() { with_bucket test_delete_ownership_controls_impl; }
test_put_request_payment() { with_bucket test_put_request_payment_impl; }
test_get_request_payment() { with_bucket test_get_request_payment_impl; }

main() {
  printf 'Running ownership controls integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put ownership controls' test_put_ownership_controls
  run_test 'Get ownership controls' test_get_ownership_controls
  run_test 'Delete ownership controls' test_delete_ownership_controls
  run_test 'Put request payment configuration' test_put_request_payment
  run_test 'Get request payment configuration' test_get_request_payment

  printf 'Ownership tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
