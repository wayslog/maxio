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

export S3_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION AWS_EC2_METADATA_DISABLED
export AWS_MAX_ATTEMPTS AWS_RETRY_MODE AWS_CLI_CONNECT_TIMEOUT AWS_CLI_READ_TIMEOUT

TEST_SCRIPTS=(
  "$SCRIPT_DIR/test_buckets.sh"
  "$SCRIPT_DIR/test_objects.sh"
  "$SCRIPT_DIR/test_copy.sh"
  "$SCRIPT_DIR/test_delete_objects.sh"
  "$SCRIPT_DIR/test_acl.sh"
  "$SCRIPT_DIR/test_multipart.sh"
  "$SCRIPT_DIR/test_versioning.sh"
  "$SCRIPT_DIR/test_tagging.sh"
  "$SCRIPT_DIR/test_lifecycle.sh"
  "$SCRIPT_DIR/test_cors.sh"
  "$SCRIPT_DIR/test_encryption.sh"
  "$SCRIPT_DIR/test_policy.sh"
  "$SCRIPT_DIR/test_website.sh"
  "$SCRIPT_DIR/test_logging.sh"
  "$SCRIPT_DIR/test_object_lock.sh"
  "$SCRIPT_DIR/test_public_access.sh"
  "$SCRIPT_DIR/test_ownership.sh"
  "$SCRIPT_DIR/test_conditional.sh"
)

PASS_COUNT=0
FAIL_COUNT=0

run_suite() {
  local script_path="$1"
  local script_name
  script_name="$(basename "$script_path")"

  printf '\n==> Running %s\n' "$script_name"
  if bash "$script_path"; then
    PASS_COUNT=$((PASS_COUNT + 1))
    printf '==> PASS %s\n' "$script_name"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    printf '==> FAIL %s\n' "$script_name"
  fi
}

main() {
  printf 'S3 integration test runner\n'
  printf 'Endpoint: %s\n' "$S3_ENDPOINT_URL"

  for script in "${TEST_SCRIPTS[@]}"; do
    if [ ! -x "$script" ]; then
      printf 'Missing executable test script: %s\n' "$script"
      FAIL_COUNT=$((FAIL_COUNT + 1))
      continue
    fi
    run_suite "$script"
  done

  printf '\n==============================\n'
  printf 'Integration test summary\n'
  printf 'Passed suites: %s\n' "$PASS_COUNT"
  printf 'Failed suites: %s\n' "$FAIL_COUNT"
  printf '==============================\n'

  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
