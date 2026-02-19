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
  printf 'maxio-it-object-%s-%s' "$(date +%s)" "$RANDOM"
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

test_put_object_impl() {
  local bucket="$1"
  local src="$SCRIPT_DIR/.tmp-put-$RANDOM.txt"

  printf 'hello-object-put' >"$src"
  aws s3 cp "$src" "s3://$bucket/put.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3api head-object --bucket "$bucket" --key 'put.txt' "${AWS_ARGS[@]}" >/dev/null

  rm -f "$src"
}

test_get_object_impl() {
  local bucket="$1"
  local src="$SCRIPT_DIR/.tmp-get-src-$RANDOM.txt"
  local dst="$SCRIPT_DIR/.tmp-get-dst-$RANDOM.txt"

  printf 'hello-object-get' >"$src"
  aws s3 cp "$src" "s3://$bucket/get.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3 cp "s3://$bucket/get.txt" "$dst" "${AWS_ARGS[@]}" >/dev/null

  cmp -s "$src" "$dst"
  rm -f "$src" "$dst"
}

test_head_object_impl() {
  local bucket="$1"
  local src="$SCRIPT_DIR/.tmp-head-$RANDOM.txt"

  printf 'hello-head' >"$src"
  aws s3 cp "$src" "s3://$bucket/head.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3api head-object --bucket "$bucket" --key 'head.txt' "${AWS_ARGS[@]}" >/dev/null

  rm -f "$src"
}

test_delete_object_impl() {
  local bucket="$1"
  local src="$SCRIPT_DIR/.tmp-delete-$RANDOM.txt"

  printf 'hello-delete' >"$src"
  aws s3 cp "$src" "s3://$bucket/delete.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3 rm "s3://$bucket/delete.txt" "${AWS_ARGS[@]}" >/dev/null

  if aws s3api head-object --bucket "$bucket" --key 'delete.txt' "${AWS_ARGS[@]}" >/dev/null 2>&1; then
    rm -f "$src"
    return 1
  fi

  rm -f "$src"
}

test_list_objects_v1_impl() {
  local bucket="$1"
  local src="$SCRIPT_DIR/.tmp-listv1-$RANDOM.txt"
  local found

  printf 'hello-list-v1' >"$src"
  aws s3 cp "$src" "s3://$bucket/list-v1.txt" "${AWS_ARGS[@]}" >/dev/null
  found="$(aws s3api list-objects --bucket "$bucket" --query 'Contents[?Key==`list-v1.txt`].Key' --output text "${AWS_ARGS[@]}")"

  rm -f "$src"
  [ "$found" = 'list-v1.txt' ]
}

test_list_objects_v2_impl() {
  local bucket="$1"
  local src="$SCRIPT_DIR/.tmp-listv2-$RANDOM.txt"
  local found

  printf 'hello-list-v2' >"$src"
  aws s3 cp "$src" "s3://$bucket/list-v2.txt" "${AWS_ARGS[@]}" >/dev/null
  found="$(aws s3api list-objects-v2 --bucket "$bucket" --query 'Contents[?Key==`list-v2.txt`].Key' --output text "${AWS_ARGS[@]}")"

  rm -f "$src"
  [ "$found" = 'list-v2.txt' ]
}

test_put_object_with_metadata_impl() {
  local bucket="$1"
  local src="$SCRIPT_DIR/.tmp-meta-$RANDOM.txt"
  local team

  printf 'hello-metadata' >"$src"
  aws s3 cp "$src" "s3://$bucket/meta.txt" --metadata 'team=platform,env=it' "${AWS_ARGS[@]}" >/dev/null
  team="$(aws s3api head-object --bucket "$bucket" --key 'meta.txt' --query 'Metadata.team' --output text "${AWS_ARGS[@]}")"

  rm -f "$src"
  [ "$team" = 'platform' ]
}

test_get_object_range_impl() {
  local bucket="$1"
  local src="$SCRIPT_DIR/.tmp-range-src-$RANDOM.txt"
  local dst="$SCRIPT_DIR/.tmp-range-dst-$RANDOM.txt"

  printf '0123456789' >"$src"
  aws s3 cp "$src" "s3://$bucket/range.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3api get-object --bucket "$bucket" --key 'range.txt' --range 'bytes=0-4' "$dst" "${AWS_ARGS[@]}" >/dev/null

  if [ "$(cat "$dst")" != '01234' ]; then
    rm -f "$src" "$dst"
    return 1
  fi

  rm -f "$src" "$dst"
}

test_put_object() { with_bucket test_put_object_impl; }
test_get_object() { with_bucket test_get_object_impl; }
test_head_object() { with_bucket test_head_object_impl; }
test_delete_object() { with_bucket test_delete_object_impl; }
test_list_objects_v1() { with_bucket test_list_objects_v1_impl; }
test_list_objects_v2() { with_bucket test_list_objects_v2_impl; }
test_put_object_with_metadata() { with_bucket test_put_object_with_metadata_impl; }
test_get_object_range() { with_bucket test_get_object_range_impl; }

main() {
  printf 'Running object integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put object (aws s3 cp)' test_put_object
  run_test 'Get object (aws s3 cp from s3)' test_get_object
  run_test 'Head object (aws s3api head-object)' test_head_object
  run_test 'Delete object (aws s3 rm)' test_delete_object
  run_test 'List objects v1 (aws s3api list-objects)' test_list_objects_v1
  run_test 'List objects v2 (aws s3api list-objects-v2)' test_list_objects_v2
  run_test 'Put object with metadata (--metadata)' test_put_object_with_metadata
  run_test 'Get object with range (aws s3api get-object --range)' test_get_object_range

  printf 'Object tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
