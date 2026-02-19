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
  printf 'maxio-it-delete-objects-%s-%s' "$(date +%s)" "$RANDOM"
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

create_object() {
  local bucket="$1"
  local key="$2"
  local src_file="$SCRIPT_DIR/.tmp-delete-$RANDOM.txt"

  printf 'deletable-%s' "$key" >"$src_file"
  aws s3 cp "$src_file" "s3://$bucket/$key" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$src_file"
}

assert_object_missing() {
  local bucket="$1"
  local key="$2"

  if aws s3api head-object --bucket "$bucket" --key "$key" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
    return 1
  fi

  return 0
}

test_delete_multiple_objects_impl() {
  local bucket="$1"
  local deleted_count

  create_object "$bucket" 'batch-1.txt'
  create_object "$bucket" 'batch-2.txt'
  create_object "$bucket" 'batch-3.txt'

  deleted_count="$(aws s3api delete-objects --bucket "$bucket" --delete 'Objects=[{Key=batch-1.txt},{Key=batch-2.txt},{Key=batch-3.txt}]' --query 'length(Deleted)' --output text "${AWS_ARGS[@]}")"

  [ "$deleted_count" -eq 3 ]
}

test_delete_with_quiet_mode_impl() {
  local bucket="$1"
  local result

  create_object "$bucket" 'quiet-1.txt'
  create_object "$bucket" 'quiet-2.txt'

  result="$(aws s3api delete-objects --bucket "$bucket" --delete 'Objects=[{Key=quiet-1.txt},{Key=quiet-2.txt}],Quiet=true' --output json "${AWS_ARGS[@]}")"

  assert_object_missing "$bucket" 'quiet-1.txt'
  assert_object_missing "$bucket" 'quiet-2.txt'
}

test_delete_non_existent_objects_impl() {
  local bucket="$1"
  local deleted_count

  deleted_count="$(aws s3api delete-objects --bucket "$bucket" --delete 'Objects=[{Key=missing-1.txt},{Key=missing-2.txt}]' --query 'length(Deleted)' --output text "${AWS_ARGS[@]}")"

  [ "$deleted_count" -eq 2 ]
}

test_verify_objects_deleted_impl() {
  local bucket="$1"

  create_object "$bucket" 'verify-1.txt'
  create_object "$bucket" 'verify-2.txt'

  aws s3api delete-objects --bucket "$bucket" --delete 'Objects=[{Key=verify-1.txt},{Key=verify-2.txt}]' "${AWS_ARGS[@]}" >/dev/null
  assert_object_missing "$bucket" 'verify-1.txt'
  assert_object_missing "$bucket" 'verify-2.txt'
}

test_delete_multiple_objects() { with_bucket test_delete_multiple_objects_impl; }
test_delete_with_quiet_mode() { with_bucket test_delete_with_quiet_mode_impl; }
test_delete_non_existent_objects() { with_bucket test_delete_non_existent_objects_impl; }
test_verify_objects_deleted() { with_bucket test_verify_objects_deleted_impl; }

main() {
  printf 'Running delete-objects integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Delete multiple objects in one request' test_delete_multiple_objects
  run_test 'Delete objects with Quiet mode' test_delete_with_quiet_mode
  run_test 'Delete non-existent objects does not error' test_delete_non_existent_objects
  run_test 'Verify objects are deleted after delete-objects' test_verify_objects_deleted

  printf 'Delete-objects tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
