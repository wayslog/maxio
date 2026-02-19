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
  printf 'maxio-it-acl-%s-%s' "$(date +%s)" "$RANDOM"
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

with_bucket_and_object() {
  local bucket
  local object_file="$SCRIPT_DIR/.tmp-acl-$RANDOM.txt"
  bucket="$(create_bucket_name)"

  trap 'rm -f "$object_file"; cleanup_bucket "$bucket"' RETURN

  printf 'acl-test-object' >"$object_file"
  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3 cp "$object_file" "s3://$bucket/acl.txt" "${AWS_ARGS[@]}" >/dev/null
  "$1" "$bucket"
}

verify_acl_structure() {
  local owner_id_len="$1"
  local grants_len="$2"

  [ "$owner_id_len" -gt 0 ] && [ "$grants_len" -gt 0 ]
}

test_get_default_bucket_acl_impl() {
  local bucket="$1"
  local owner_id_len
  local grants_len

  owner_id_len="$(aws s3api get-bucket-acl --bucket "$bucket" --query 'length(Owner.ID)' --output text "${AWS_ARGS[@]}")"
  grants_len="$(aws s3api get-bucket-acl --bucket "$bucket" --query 'length(Grants)' --output text "${AWS_ARGS[@]}")"

  verify_acl_structure "$owner_id_len" "$grants_len"
}

test_put_bucket_acl_private_and_public_read_impl() {
  local bucket="$1"
  local public_grant_count
  local private_public_grant_count

  aws s3api put-bucket-acl --bucket "$bucket" --acl private "${AWS_ARGS[@]}" >/dev/null
  private_public_grant_count="$(aws s3api get-bucket-acl --bucket "$bucket" --query "length(Grants[?Grantee.Type=='Group' && contains(Grantee.URI, 'AllUsers')])" --output text "${AWS_ARGS[@]}")"

  aws s3api put-bucket-acl --bucket "$bucket" --acl public-read "${AWS_ARGS[@]}" >/dev/null
  public_grant_count="$(aws s3api get-bucket-acl --bucket "$bucket" --query "length(Grants[?Grantee.Type=='Group' && contains(Grantee.URI, 'AllUsers') && Permission=='READ'])" --output text "${AWS_ARGS[@]}")"

  [ "$private_public_grant_count" -eq 0 ] && [ "$public_grant_count" -eq 1 ]
}

test_get_object_acl_impl() {
  local bucket="$1"
  local owner_id_len
  local grants_len

  owner_id_len="$(aws s3api get-object-acl --bucket "$bucket" --key 'acl.txt' --query 'length(Owner.ID)' --output text "${AWS_ARGS[@]}")"
  grants_len="$(aws s3api get-object-acl --bucket "$bucket" --key 'acl.txt' --query 'length(Grants)' --output text "${AWS_ARGS[@]}")"

  verify_acl_structure "$owner_id_len" "$grants_len"
}

test_put_object_acl_impl() {
  local bucket="$1"
  local public_read_grant_count

  aws s3api put-object-acl --bucket "$bucket" --key 'acl.txt' --acl public-read "${AWS_ARGS[@]}" >/dev/null
  public_read_grant_count="$(aws s3api get-object-acl --bucket "$bucket" --key 'acl.txt' --query "length(Grants[?Grantee.Type=='Group' && contains(Grantee.URI, 'AllUsers') && Permission=='READ'])" --output text "${AWS_ARGS[@]}")"

  [ "$public_read_grant_count" -eq 1 ]
}

test_verify_acl_structure_impl() {
  local bucket="$1"
  local bucket_permission_count
  local object_permission_count

  bucket_permission_count="$(aws s3api get-bucket-acl --bucket "$bucket" --query 'length(Grants[?Permission!=`null`])' --output text "${AWS_ARGS[@]}")"
  object_permission_count="$(aws s3api get-object-acl --bucket "$bucket" --key 'acl.txt' --query 'length(Grants[?Permission!=`null`])' --output text "${AWS_ARGS[@]}")"

  [ "$bucket_permission_count" -gt 0 ] && [ "$object_permission_count" -gt 0 ]
}

test_get_default_bucket_acl() { with_bucket test_get_default_bucket_acl_impl; }
test_put_bucket_acl_private_and_public_read() { with_bucket test_put_bucket_acl_private_and_public_read_impl; }
test_get_object_acl() { with_bucket_and_object test_get_object_acl_impl; }
test_put_object_acl() { with_bucket_and_object test_put_object_acl_impl; }
test_verify_acl_structure() { with_bucket_and_object test_verify_acl_structure_impl; }

main() {
  printf 'Running ACL integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Get default bucket ACL' test_get_default_bucket_acl
  run_test 'Put bucket ACL with canned ACLs (private, public-read)' test_put_bucket_acl_private_and_public_read
  run_test 'Get object ACL' test_get_object_acl
  run_test 'Put object ACL' test_put_object_acl
  run_test 'Verify ACL XML structure fields' test_verify_acl_structure

  printf 'ACL tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
