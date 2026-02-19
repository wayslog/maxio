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
  printf 'maxio-it-cors-%s-%s' "$(date +%s)" "$RANDOM"
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

write_cors_config() {
  local file="$1"
  cat >"$file" <<EOF
{
  "CORSRules": [
    {
      "AllowedHeaders": [
        "*"
      ],
      "AllowedMethods": [
        "GET",
        "PUT"
      ],
      "AllowedOrigins": [
        "https://example.com"
      ],
      "ExposeHeaders": [
        "ETag"
      ],
      "MaxAgeSeconds": 3000
    }
  ]
}
EOF
}

test_put_cors_configuration_impl() {
  local bucket="$1"
  local cors_json="$SCRIPT_DIR/.tmp-cors-put-$RANDOM.json"

  write_cors_config "$cors_json"
  aws s3api put-bucket-cors --bucket "$bucket" --cors-configuration "file://$cors_json" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$cors_json"
}

test_get_cors_configuration_impl() {
  local bucket="$1"
  local cors_json="$SCRIPT_DIR/.tmp-cors-get-$RANDOM.json"
  local origin

  write_cors_config "$cors_json"
  aws s3api put-bucket-cors --bucket "$bucket" --cors-configuration "file://$cors_json" "${AWS_ARGS[@]}" >/dev/null
  origin="$(aws s3api get-bucket-cors --bucket "$bucket" --query 'CORSRules[0].AllowedOrigins[0]' --output text "${AWS_ARGS[@]}")"

  rm -f "$cors_json"
  [ "$origin" = 'https://example.com' ]
}

test_delete_cors_configuration_impl() {
  local bucket="$1"
  local cors_json="$SCRIPT_DIR/.tmp-cors-del-$RANDOM.json"

  write_cors_config "$cors_json"
  aws s3api put-bucket-cors --bucket "$bucket" --cors-configuration "file://$cors_json" "${AWS_ARGS[@]}" >/dev/null
  aws s3api delete-bucket-cors --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null

  rm -f "$cors_json"
  if aws s3api get-bucket-cors --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
    return 1
  fi
}

test_get_cors_configuration_not_found_impl() {
  local bucket="$1"
  local stderr_file="$SCRIPT_DIR/.tmp-cors-missing-$RANDOM.stderr"

  if aws s3api get-bucket-cors --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>"$stderr_file"; then
    rm -f "$stderr_file"
    return 1
  fi

  grep -q 'NoSuchCORSConfiguration' "$stderr_file"
  rm -f "$stderr_file"
}

test_put_cors_configuration() { with_bucket test_put_cors_configuration_impl; }
test_get_cors_configuration() { with_bucket test_get_cors_configuration_impl; }
test_delete_cors_configuration() { with_bucket test_delete_cors_configuration_impl; }
test_get_cors_configuration_not_found() { with_bucket test_get_cors_configuration_not_found_impl; }

main() {
  printf 'Running CORS integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put CORS configuration (aws s3api put-bucket-cors)' test_put_cors_configuration
  run_test 'Get CORS configuration (aws s3api get-bucket-cors)' test_get_cors_configuration
  run_test 'Delete CORS configuration (aws s3api delete-bucket-cors)' test_delete_cors_configuration
  run_test 'Get missing CORS returns NoSuchCORSConfiguration (aws s3api get-bucket-cors)' test_get_cors_configuration_not_found

  printf 'CORS tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
