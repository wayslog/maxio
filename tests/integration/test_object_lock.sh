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
  printf 'maxio-it-object-lock-%s-%s' "$(date +%s)" "$RANDOM"
}

cleanup_bucket() {
  local bucket="$1"
  aws s3 rm "s3://$bucket" --recursive "${AWS_ARGS[@]}" >/dev/null 2>&1 || true

  local versions
  versions="$(aws s3api list-object-versions --bucket "$bucket" --query 'Versions[].{Key:Key,VersionId:VersionId}' --output text "${AWS_ARGS[@]}" 2>/dev/null || true)"
  if [ -n "$versions" ]; then
    while read -r key version_id; do
      [ -n "$key" ] || continue
      [ -n "$version_id" ] || continue
      aws s3api delete-object --bucket "$bucket" --key "$key" --version-id "$version_id" "${AWS_ARGS[@]}" >/dev/null 2>&1 || true
    done <<EOF
$versions
EOF
  fi

  local markers
  markers="$(aws s3api list-object-versions --bucket "$bucket" --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' --output text "${AWS_ARGS[@]}" 2>/dev/null || true)"
  if [ -n "$markers" ]; then
    while read -r key version_id; do
      [ -n "$key" ] || continue
      [ -n "$version_id" ] || continue
      aws s3api delete-object --bucket "$bucket" --key "$key" --version-id "$version_id" "${AWS_ARGS[@]}" >/dev/null 2>&1 || true
    done <<EOF
$markers
EOF
  fi

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
  local bucket
  bucket="$(create_bucket_name)"

  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3api put-object --bucket "$bucket" --key 'locked-object.txt' --body /dev/stdin "${AWS_ARGS[@]}" >/dev/null <<<"object-lock-test"

  trap 'cleanup_bucket "$bucket"' RETURN
  "$1" "$bucket"
}

write_object_lock_configuration() {
  local file="$1"
  cat >"$file" <<EOF
{
  "ObjectLockEnabled": "Enabled",
  "Rule": {
    "DefaultRetention": {
      "Mode": "GOVERNANCE",
      "Days": 7
    }
  }
}
EOF
}

assert_aws_error_code() {
  local expected_code="$1"
  shift
  local err_file="$SCRIPT_DIR/.tmp-object-lock-error-$RANDOM.txt"
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

test_put_object_lock_configuration_impl() {
  local bucket="$1"
  local object_lock_json="$SCRIPT_DIR/.tmp-object-lock-put-$RANDOM.json"

  write_object_lock_configuration "$object_lock_json"
  aws s3api put-object-lock-configuration --bucket "$bucket" --object-lock-configuration "file://$object_lock_json" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$object_lock_json"
}

test_get_object_lock_configuration_impl() {
  local bucket="$1"
  local object_lock_json="$SCRIPT_DIR/.tmp-object-lock-get-$RANDOM.json"
  local lock_enabled
  local retention_mode

  write_object_lock_configuration "$object_lock_json"
  aws s3api put-object-lock-configuration --bucket "$bucket" --object-lock-configuration "file://$object_lock_json" "${AWS_ARGS[@]}" >/dev/null

  lock_enabled="$(aws s3api get-object-lock-configuration --bucket "$bucket" --query 'ObjectLockConfiguration.ObjectLockEnabled' --output text "${AWS_ARGS[@]}")"
  retention_mode="$(aws s3api get-object-lock-configuration --bucket "$bucket" --query 'ObjectLockConfiguration.Rule.DefaultRetention.Mode' --output text "${AWS_ARGS[@]}")"

  rm -f "$object_lock_json"
  [ "$lock_enabled" = 'Enabled' ] && [ "$retention_mode" = 'GOVERNANCE' ]
}

test_put_object_legal_hold_on_off_impl() {
  local bucket="$1"
  local status

  aws s3api put-object-legal-hold --bucket "$bucket" --key 'locked-object.txt' --legal-hold 'Status=ON' "${AWS_ARGS[@]}" >/dev/null
  status="$(aws s3api get-object-legal-hold --bucket "$bucket" --key 'locked-object.txt' --query 'LegalHold.Status' --output text "${AWS_ARGS[@]}")"
  [ "$status" = 'ON' ]

  aws s3api put-object-legal-hold --bucket "$bucket" --key 'locked-object.txt' --legal-hold 'Status=OFF' "${AWS_ARGS[@]}" >/dev/null
  status="$(aws s3api get-object-legal-hold --bucket "$bucket" --key 'locked-object.txt' --query 'LegalHold.Status' --output text "${AWS_ARGS[@]}")"
  [ "$status" = 'OFF' ]
}

test_get_object_legal_hold_impl() {
  local bucket="$1"
  local status

  aws s3api put-object-legal-hold --bucket "$bucket" --key 'locked-object.txt' --legal-hold 'Status=ON' "${AWS_ARGS[@]}" >/dev/null
  status="$(aws s3api get-object-legal-hold --bucket "$bucket" --key 'locked-object.txt' --query 'LegalHold.Status' --output text "${AWS_ARGS[@]}")"
  [ "$status" = 'ON' ]
}

test_put_object_retention_impl() {
  local bucket="$1"
  local retain_until
  local mode

  retain_until="$(date -u -d '+1 day' '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -v+1d '+%Y-%m-%dT%H:%M:%SZ')"
  aws s3api put-object-retention --bucket "$bucket" --key 'locked-object.txt' --retention "Mode=GOVERNANCE,RetainUntilDate=$retain_until" "${AWS_ARGS[@]}" >/dev/null
  mode="$(aws s3api get-object-retention --bucket "$bucket" --key 'locked-object.txt' --query 'Retention.Mode' --output text "${AWS_ARGS[@]}")"
  [ "$mode" = 'GOVERNANCE' ]
}

test_get_object_retention_impl() {
  local bucket="$1"
  local retain_until
  local mode
  local returned_date

  retain_until="$(date -u -d '+1 day' '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -v+1d '+%Y-%m-%dT%H:%M:%SZ')"
  aws s3api put-object-retention --bucket "$bucket" --key 'locked-object.txt' --retention "Mode=GOVERNANCE,RetainUntilDate=$retain_until" "${AWS_ARGS[@]}" >/dev/null
  mode="$(aws s3api get-object-retention --bucket "$bucket" --key 'locked-object.txt' --query 'Retention.Mode' --output text "${AWS_ARGS[@]}")"
  returned_date="$(aws s3api get-object-retention --bucket "$bucket" --key 'locked-object.txt' --query 'Retention.RetainUntilDate' --output text "${AWS_ARGS[@]}")"

  [ "$mode" = 'GOVERNANCE' ] && [ -n "$returned_date" ]
}

test_object_lock_configuration_not_found_impl() {
  local bucket="$1"
  assert_aws_error_code 'ObjectLockConfigurationNotFoundError' aws s3api get-object-lock-configuration --bucket "$bucket" "${AWS_ARGS[@]}"
}

test_put_object_lock_configuration() { with_bucket_and_object test_put_object_lock_configuration_impl; }
test_get_object_lock_configuration() { with_bucket_and_object test_get_object_lock_configuration_impl; }
test_put_object_legal_hold_on_off() { with_bucket_and_object test_put_object_legal_hold_on_off_impl; }
test_get_object_legal_hold() { with_bucket_and_object test_get_object_legal_hold_impl; }
test_put_object_retention() { with_bucket_and_object test_put_object_retention_impl; }
test_get_object_retention() { with_bucket_and_object test_get_object_retention_impl; }
test_object_lock_configuration_not_found() { with_bucket_and_object test_object_lock_configuration_not_found_impl; }

main() {
  printf 'Running object lock integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put object lock configuration (aws s3api put-object-lock-configuration)' test_put_object_lock_configuration
  run_test 'Get object lock configuration (aws s3api get-object-lock-configuration)' test_get_object_lock_configuration
  run_test 'Put object legal hold ON/OFF (aws s3api put-object-legal-hold)' test_put_object_legal_hold_on_off
  run_test 'Get object legal hold (aws s3api get-object-legal-hold)' test_get_object_legal_hold
  run_test 'Put object retention (aws s3api put-object-retention)' test_put_object_retention
  run_test 'Get object retention (aws s3api get-object-retention)' test_get_object_retention
  run_test 'Get object lock config returns ObjectLockConfigurationNotFoundError when unset' test_object_lock_configuration_not_found

  printf 'Object lock tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
