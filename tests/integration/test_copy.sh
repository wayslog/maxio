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
  printf 'maxio-it-copy-%s-%s' "$(date +%s)" "$RANDOM"
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

with_two_buckets() {
  local src_bucket
  local dst_bucket
  src_bucket="$(create_bucket_name)"
  dst_bucket="$(create_bucket_name)"

  aws s3 mb "s3://$src_bucket" "${AWS_ARGS[@]}" >/dev/null
  aws s3 mb "s3://$dst_bucket" "${AWS_ARGS[@]}" >/dev/null
  trap 'cleanup_bucket "$src_bucket"; cleanup_bucket "$dst_bucket"' RETURN
  "$1" "$src_bucket" "$dst_bucket"
}

test_copy_within_bucket_impl() {
  local bucket="$1"
  local src_file="$SCRIPT_DIR/.tmp-copy-src-$RANDOM.txt"

  printf 'copy-within-bucket' >"$src_file"
  aws s3 cp "$src_file" "s3://$bucket/source.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3api copy-object --bucket "$bucket" --key 'copied.txt' --copy-source "$bucket/source.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3api head-object --bucket "$bucket" --key 'copied.txt' "${AWS_ARGS[@]}" >/dev/null

  rm -f "$src_file"
}

test_copy_to_different_bucket_impl() {
  local src_bucket="$1"
  local dst_bucket="$2"
  local src_file="$SCRIPT_DIR/.tmp-cross-copy-$RANDOM.txt"

  printf 'copy-across-buckets' >"$src_file"
  aws s3 cp "$src_file" "s3://$src_bucket/source.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3api copy-object --bucket "$dst_bucket" --key 'copied.txt' --copy-source "$src_bucket/source.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3api head-object --bucket "$dst_bucket" --key 'copied.txt' "${AWS_ARGS[@]}" >/dev/null

  rm -f "$src_file"
}

test_copy_with_metadata_copy_impl() {
  local bucket="$1"
  local src_file="$SCRIPT_DIR/.tmp-meta-copy-$RANDOM.txt"
  local team

  printf 'copy-metadata-copy' >"$src_file"
  aws s3 cp "$src_file" "s3://$bucket/source.txt" --metadata 'team=platform' "${AWS_ARGS[@]}" >/dev/null
  aws s3api copy-object --bucket "$bucket" --key 'copied-copy.txt' --copy-source "$bucket/source.txt" --metadata-directive COPY "${AWS_ARGS[@]}" >/dev/null
  team="$(aws s3api head-object --bucket "$bucket" --key 'copied-copy.txt' --query 'Metadata.team' --output text "${AWS_ARGS[@]}")"

  rm -f "$src_file"
  [ "$team" = 'platform' ]
}

test_copy_with_metadata_replace_impl() {
  local bucket="$1"
  local src_file="$SCRIPT_DIR/.tmp-meta-replace-$RANDOM.txt"
  local team
  local env

  printf 'copy-metadata-replace' >"$src_file"
  aws s3 cp "$src_file" "s3://$bucket/source.txt" --metadata 'team=platform' "${AWS_ARGS[@]}" >/dev/null
  aws s3api copy-object --bucket "$bucket" --key 'copied-replace.txt' --copy-source "$bucket/source.txt" --metadata-directive REPLACE --metadata 'team=qa,env=it' "${AWS_ARGS[@]}" >/dev/null
  team="$(aws s3api head-object --bucket "$bucket" --key 'copied-replace.txt' --query 'Metadata.team' --output text "${AWS_ARGS[@]}")"
  env="$(aws s3api head-object --bucket "$bucket" --key 'copied-replace.txt' --query 'Metadata.env' --output text "${AWS_ARGS[@]}")"

  rm -f "$src_file"
  [ "$team" = 'qa' ] && [ "$env" = 'it' ]
}

test_copy_with_if_match_impl() {
  local bucket="$1"
  local src_file="$SCRIPT_DIR/.tmp-ifmatch-$RANDOM.txt"
  local etag

  printf 'copy-if-match' >"$src_file"
  aws s3 cp "$src_file" "s3://$bucket/source.txt" "${AWS_ARGS[@]}" >/dev/null
  etag="$(aws s3api head-object --bucket "$bucket" --key 'source.txt' --query 'ETag' --output text "${AWS_ARGS[@]}")"
  aws s3api copy-object --bucket "$bucket" --key 'copied-if-match.txt' --copy-source "$bucket/source.txt" --copy-source-if-match "$etag" "${AWS_ARGS[@]}" >/dev/null
  aws s3api head-object --bucket "$bucket" --key 'copied-if-match.txt' "${AWS_ARGS[@]}" >/dev/null

  rm -f "$src_file"
}

test_copy_content_matches_source_impl() {
  local bucket="$1"
  local src_file="$SCRIPT_DIR/.tmp-content-src-$RANDOM.txt"
  local dst_file="$SCRIPT_DIR/.tmp-content-dst-$RANDOM.txt"

  printf 'copy-content-verification' >"$src_file"
  aws s3 cp "$src_file" "s3://$bucket/source.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3api copy-object --bucket "$bucket" --key 'copied-content.txt' --copy-source "$bucket/source.txt" "${AWS_ARGS[@]}" >/dev/null
  aws s3 cp "s3://$bucket/copied-content.txt" "$dst_file" "${AWS_ARGS[@]}" >/dev/null

  cmp -s "$src_file" "$dst_file"
  rm -f "$src_file" "$dst_file"
}

test_copy_within_bucket() { with_bucket test_copy_within_bucket_impl; }
test_copy_to_different_bucket() { with_two_buckets test_copy_to_different_bucket_impl; }
test_copy_with_metadata_copy() { with_bucket test_copy_with_metadata_copy_impl; }
test_copy_with_metadata_replace() { with_bucket test_copy_with_metadata_replace_impl; }
test_copy_with_if_match() { with_bucket test_copy_with_if_match_impl; }
test_copy_content_matches_source() { with_bucket test_copy_content_matches_source_impl; }

main() {
  printf 'Running copy integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Copy object within same bucket (aws s3api copy-object)' test_copy_within_bucket
  run_test 'Copy object to different bucket (aws s3api copy-object)' test_copy_to_different_bucket
  run_test 'Copy with metadata directive COPY' test_copy_with_metadata_copy
  run_test 'Copy with metadata directive REPLACE' test_copy_with_metadata_replace
  run_test 'Copy with conditional if-match header' test_copy_with_if_match
  run_test 'Verify copied object content matches source' test_copy_content_matches_source

  printf 'Copy tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
