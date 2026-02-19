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
  printf 'maxio-it-tag-%s-%s' "$(date +%s)" "$RANDOM"
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

with_bucket_and_object() {
  local bucket
  local object_file="$SCRIPT_DIR/.tmp-tag-object-$RANDOM.txt"

  bucket="$(create_bucket_name)"
  printf 'taggable-object' >"$object_file"

  aws s3 mb "s3://$bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3 cp "$object_file" "s3://$bucket/taggable.txt" "${AWS_ARGS[@]}" >/dev/null

  trap 'rm -f "$object_file"; cleanup_bucket "$bucket"' RETURN
  "$1" "$bucket"
}

test_put_object_tagging_impl() {
  local bucket="$1"
  local env_tag

  aws s3api put-object-tagging --bucket "$bucket" --key 'taggable.txt' --tagging 'TagSet=[{Key=env,Value=test},{Key=team,Value=qa}]' "${AWS_ARGS[@]}" >/dev/null
  env_tag="$(aws s3api get-object-tagging --bucket "$bucket" --key 'taggable.txt' --query 'TagSet[?Key==`env`].Value' --output text "${AWS_ARGS[@]}")"
  [ "$env_tag" = 'test' ]
}

test_get_object_tagging_impl() {
  local bucket="$1"
  local tag_count

  aws s3api put-object-tagging --bucket "$bucket" --key 'taggable.txt' --tagging 'TagSet=[{Key=feature,Value=tagging}]' "${AWS_ARGS[@]}" >/dev/null
  tag_count="$(aws s3api get-object-tagging --bucket "$bucket" --key 'taggable.txt' --query 'length(TagSet)' --output text "${AWS_ARGS[@]}")"
  [ "$tag_count" -eq 1 ]
}

test_delete_object_tagging_impl() {
  local bucket="$1"
  local tag_count

  aws s3api put-object-tagging --bucket "$bucket" --key 'taggable.txt' --tagging 'TagSet=[{Key=cleanup,Value=yes}]' "${AWS_ARGS[@]}" >/dev/null
  aws s3api delete-object-tagging --bucket "$bucket" --key 'taggable.txt' "${AWS_ARGS[@]}" >/dev/null
  tag_count="$(aws s3api get-object-tagging --bucket "$bucket" --key 'taggable.txt' --query 'length(TagSet)' --output text "${AWS_ARGS[@]}")"
  [ "$tag_count" -eq 0 ]
}

test_put_object_tagging() { with_bucket_and_object test_put_object_tagging_impl; }
test_get_object_tagging() { with_bucket_and_object test_get_object_tagging_impl; }
test_delete_object_tagging() { with_bucket_and_object test_delete_object_tagging_impl; }

main() {
  printf 'Running tagging integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put object tagging (aws s3api put-object-tagging)' test_put_object_tagging
  run_test 'Get object tagging (aws s3api get-object-tagging)' test_get_object_tagging
  run_test 'Delete object tagging (aws s3api delete-object-tagging)' test_delete_object_tagging

  printf 'Tagging tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
