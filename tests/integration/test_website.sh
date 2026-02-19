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
  printf 'maxio-it-website-%s-%s' "$(date +%s)" "$RANDOM"
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

write_website_config() {
  local file="$1"
  cat >"$file" <<'EOF'
{
  "IndexDocument": {
    "Suffix": "index.html"
  },
  "ErrorDocument": {
    "Key": "error.html"
  }
}
EOF
}

assert_aws_error_code() {
  local expected_code="$1"
  shift
  local err_file="$SCRIPT_DIR/.tmp-website-error-$RANDOM.txt"
  local status

  set +e
  "$@" >/dev/null 2>"$err_file"
  status=$?
  set -e

  if [ "$status" -eq 0 ]; then
    rm -f "$err_file"
    return 1
  fi

  if ! grep -q "$expected_code" "$err_file"; then
    rm -f "$err_file"
    return 1
  fi

  rm -f "$err_file"
}

test_put_website_configuration_impl() {
  local bucket="$1"
  local website_json="$SCRIPT_DIR/.tmp-website-put-$RANDOM.json"

  write_website_config "$website_json"
  aws s3api put-bucket-website --bucket "$bucket" --website-configuration "file://$website_json" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$website_json"
}

test_get_website_configuration_impl() {
  local bucket="$1"
  local website_json="$SCRIPT_DIR/.tmp-website-get-$RANDOM.json"
  local suffix

  write_website_config "$website_json"
  aws s3api put-bucket-website --bucket "$bucket" --website-configuration "file://$website_json" "${AWS_ARGS[@]}" >/dev/null
  suffix="$(aws s3api get-bucket-website --bucket "$bucket" --query 'IndexDocument.Suffix' --output text "${AWS_ARGS[@]}")"

  rm -f "$website_json"
  [ "$suffix" = 'index.html' ]
}

test_delete_website_configuration_impl() {
  local bucket="$1"
  local website_json="$SCRIPT_DIR/.tmp-website-del-$RANDOM.json"

  write_website_config "$website_json"
  aws s3api put-bucket-website --bucket "$bucket" --website-configuration "file://$website_json" "${AWS_ARGS[@]}" >/dev/null
  aws s3api delete-bucket-website --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null

  rm -f "$website_json"
  assert_aws_error_code 'NoSuchWebsiteConfiguration' aws s3api get-bucket-website --bucket "$bucket" "${AWS_ARGS[@]}"
}

test_missing_website_configuration_error_impl() {
  local bucket="$1"
  assert_aws_error_code 'NoSuchWebsiteConfiguration' aws s3api get-bucket-website --bucket "$bucket" "${AWS_ARGS[@]}"
}

test_put_website_configuration() { with_bucket test_put_website_configuration_impl; }
test_get_website_configuration() { with_bucket test_get_website_configuration_impl; }
test_delete_website_configuration() { with_bucket test_delete_website_configuration_impl; }
test_missing_website_configuration_error() { with_bucket test_missing_website_configuration_error_impl; }

main() {
  printf 'Running website integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put website configuration (aws s3api put-bucket-website)' test_put_website_configuration
  run_test 'Get website configuration (aws s3api get-bucket-website)' test_get_website_configuration
  run_test 'Delete website configuration (aws s3api delete-bucket-website)' test_delete_website_configuration
  run_test 'Get website returns NoSuchWebsiteConfiguration when not set' test_missing_website_configuration_error

  printf 'Website tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
