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
  printf 'maxio-it-lifecycle-%s-%s' "$(date +%s)" "$RANDOM"
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

write_lifecycle_config() {
  local file="$1"
  cat >"$file" <<EOF
{
  "Rules": [
    {
      "ID": "expire-rule",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "logs/"
      },
      "Expiration": {
        "Days": 30
      }
    }
  ]
}
EOF
}

test_put_lifecycle_configuration_impl() {
  local bucket="$1"
  local lifecycle_json="$SCRIPT_DIR/.tmp-lifecycle-put-$RANDOM.json"

  write_lifecycle_config "$lifecycle_json"
  aws s3api put-bucket-lifecycle-configuration --bucket "$bucket" --lifecycle-configuration "file://$lifecycle_json" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$lifecycle_json"
}

test_get_lifecycle_configuration_impl() {
  local bucket="$1"
  local lifecycle_json="$SCRIPT_DIR/.tmp-lifecycle-get-$RANDOM.json"
  local rule_id

  write_lifecycle_config "$lifecycle_json"
  aws s3api put-bucket-lifecycle-configuration --bucket "$bucket" --lifecycle-configuration "file://$lifecycle_json" "${AWS_ARGS[@]}" >/dev/null
  rule_id="$(aws s3api get-bucket-lifecycle-configuration --bucket "$bucket" --query 'Rules[0].ID' --output text "${AWS_ARGS[@]}")"

  rm -f "$lifecycle_json"
  [ "$rule_id" = 'expire-rule' ]
}

test_delete_lifecycle_configuration_impl() {
  local bucket="$1"
  local lifecycle_json="$SCRIPT_DIR/.tmp-lifecycle-del-$RANDOM.json"

  write_lifecycle_config "$lifecycle_json"
  aws s3api put-bucket-lifecycle-configuration --bucket "$bucket" --lifecycle-configuration "file://$lifecycle_json" "${AWS_ARGS[@]}" >/dev/null
  aws s3api delete-bucket-lifecycle --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null

  rm -f "$lifecycle_json"
  if aws s3api get-bucket-lifecycle-configuration --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
    return 1
  fi
}

test_put_lifecycle_configuration() { with_bucket test_put_lifecycle_configuration_impl; }
test_get_lifecycle_configuration() { with_bucket test_get_lifecycle_configuration_impl; }
test_delete_lifecycle_configuration() { with_bucket test_delete_lifecycle_configuration_impl; }

main() {
  printf 'Running lifecycle integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put lifecycle configuration (aws s3api put-bucket-lifecycle-configuration)' test_put_lifecycle_configuration
  run_test 'Get lifecycle configuration (aws s3api get-bucket-lifecycle-configuration)' test_get_lifecycle_configuration
  run_test 'Delete lifecycle configuration (aws s3api delete-bucket-lifecycle)' test_delete_lifecycle_configuration

  printf 'Lifecycle tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
