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
  printf 'maxio-it-version-%s-%s' "$(date +%s)" "$RANDOM"
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

with_bucket() {
  local bucket
  bucket="$(create_bucket_name)"
  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  trap 'cleanup_bucket "$bucket"' RETURN
  "$1" "$bucket"
}

enable_versioning() {
  local bucket="$1"
  aws s3api put-bucket-versioning --bucket "$bucket" --versioning-configuration 'Status=Enabled' "${AWS_ARGS[@]}" >/dev/null
}

test_enable_and_get_versioning_status_impl() {
  local bucket="$1"
  local status

  enable_versioning "$bucket"
  status="$(aws s3api get-bucket-versioning --bucket "$bucket" --query 'Status' --output text "${AWS_ARGS[@]}")"
  [ "$status" = 'Enabled' ]
}

test_upload_multiple_versions_and_list_impl() {
  local bucket="$1"
  local v1 v2 count

  enable_versioning "$bucket"
  v1="$(aws s3api put-object --bucket "$bucket" --key 'versioned.txt' --body /dev/stdin --query 'VersionId' --output text "${AWS_ARGS[@]}" <<<"first-version")"
  v2="$(aws s3api put-object --bucket "$bucket" --key 'versioned.txt' --body /dev/stdin --query 'VersionId' --output text "${AWS_ARGS[@]}" <<<"second-version")"
  count="$(aws s3api list-object-versions --bucket "$bucket" --prefix 'versioned.txt' --query 'length(Versions[?Key==`versioned.txt`])' --output text "${AWS_ARGS[@]}")"

  [ "$v1" != 'None' ] && [ "$v2" != 'None' ] && [ "$v1" != "$v2" ] && [ "$count" -ge 2 ]
}

test_get_specific_version_impl() {
  local bucket="$1"
  local version_id downloaded
  local out_file="$SCRIPT_DIR/.tmp-version-get-$RANDOM.txt"

  enable_versioning "$bucket"
  version_id="$(aws s3api put-object --bucket "$bucket" --key 'specific.txt' --body /dev/stdin --query 'VersionId' --output text "${AWS_ARGS[@]}" <<<"content-v1")"
  aws s3api put-object --bucket "$bucket" --key 'specific.txt' --body /dev/stdin "${AWS_ARGS[@]}" >/dev/null <<<"content-v2"

  aws s3api get-object --bucket "$bucket" --key 'specific.txt' --version-id "$version_id" "$out_file" "${AWS_ARGS[@]}" >/dev/null
  downloaded="$(cat "$out_file")"
  rm -f "$out_file"

  [ "$downloaded" = 'content-v1' ]
}

test_delete_specific_version_impl() {
  local bucket="$1"
  local v1 before after

  enable_versioning "$bucket"
  v1="$(aws s3api put-object --bucket "$bucket" --key 'delete-version.txt' --body /dev/stdin --query 'VersionId' --output text "${AWS_ARGS[@]}" <<<"to-delete")"
  aws s3api put-object --bucket "$bucket" --key 'delete-version.txt' --body /dev/stdin "${AWS_ARGS[@]}" >/dev/null <<<"keep"

  before="$(aws s3api list-object-versions --bucket "$bucket" --prefix 'delete-version.txt' --query "length(Versions[?VersionId=='$v1'])" --output text "${AWS_ARGS[@]}")"
  aws s3api delete-object --bucket "$bucket" --key 'delete-version.txt' --version-id "$v1" "${AWS_ARGS[@]}" >/dev/null
  after="$(aws s3api list-object-versions --bucket "$bucket" --prefix 'delete-version.txt' --query "length(Versions[?VersionId=='$v1'])" --output text "${AWS_ARGS[@]}")"

  [ "$before" -eq 1 ] && [ "$after" -eq 0 ]
}

test_enable_and_get_versioning_status() { with_bucket test_enable_and_get_versioning_status_impl; }
test_upload_multiple_versions_and_list() { with_bucket test_upload_multiple_versions_and_list_impl; }
test_get_specific_version() { with_bucket test_get_specific_version_impl; }
test_delete_specific_version() { with_bucket test_delete_specific_version_impl; }

main() {
  printf 'Running versioning integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Enable versioning and verify status' test_enable_and_get_versioning_status
  run_test 'Upload multiple versions and list versions' test_upload_multiple_versions_and_list
  run_test 'Get object by explicit version-id' test_get_specific_version
  run_test 'Delete explicit object version' test_delete_specific_version

  printf 'Versioning tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
