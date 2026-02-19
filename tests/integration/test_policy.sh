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
  printf 'maxio-it-policy-%s-%s' "$(date +%s)" "$RANDOM"
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

write_policy_config() {
  local file="$1"
  local bucket="$2"
  cat >"$file" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicRead",
      "Effect": "Allow",
      "Principal": "*",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::$bucket/*"
      ]
    }
  ]
}
EOF
}

test_put_bucket_policy_impl() {
  local bucket="$1"
  local policy_json="$SCRIPT_DIR/.tmp-policy-put-$RANDOM.json"

  write_policy_config "$policy_json" "$bucket"
  aws s3api put-bucket-policy --bucket "$bucket" --policy "file://$policy_json" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$policy_json"
}

test_get_bucket_policy_impl() {
  local bucket="$1"
  local policy_json="$SCRIPT_DIR/.tmp-policy-get-$RANDOM.json"
  local policy_text

  write_policy_config "$policy_json" "$bucket"
  aws s3api put-bucket-policy --bucket "$bucket" --policy "file://$policy_json" "${AWS_ARGS[@]}" >/dev/null
  policy_text="$(aws s3api get-bucket-policy --bucket "$bucket" --query 'Policy' --output text "${AWS_ARGS[@]}")"

  rm -f "$policy_json"
  printf '%s' "$policy_text" | grep -q 'PublicRead'
}

test_delete_bucket_policy_impl() {
  local bucket="$1"
  local policy_json="$SCRIPT_DIR/.tmp-policy-del-$RANDOM.json"

  write_policy_config "$policy_json" "$bucket"
  aws s3api put-bucket-policy --bucket "$bucket" --policy "file://$policy_json" "${AWS_ARGS[@]}" >/dev/null
  aws s3api delete-bucket-policy --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null

  rm -f "$policy_json"
  if aws s3api get-bucket-policy --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>&1; then
    return 1
  fi
}

test_get_bucket_policy_status_impl() {
  local bucket="$1"
  local policy_json="$SCRIPT_DIR/.tmp-policy-status-$RANDOM.json"
  local is_public

  write_policy_config "$policy_json" "$bucket"
  aws s3api put-bucket-policy --bucket "$bucket" --policy "file://$policy_json" "${AWS_ARGS[@]}" >/dev/null
  is_public="$(aws s3api get-bucket-policy-status --bucket "$bucket" --query 'PolicyStatus.IsPublic' --output text "${AWS_ARGS[@]}")"

  rm -f "$policy_json"
  [ "$is_public" = 'True' ] || [ "$is_public" = 'False' ]
}

test_get_bucket_policy_not_found_impl() {
  local bucket="$1"
  local stderr_file="$SCRIPT_DIR/.tmp-policy-missing-$RANDOM.stderr"

  if aws s3api get-bucket-policy --bucket "$bucket" "${AWS_ARGS[@]}" >/dev/null 2>"$stderr_file"; then
    rm -f "$stderr_file"
    return 1
  fi

  grep -q 'NoSuchBucketPolicy' "$stderr_file"
  rm -f "$stderr_file"
}

test_put_bucket_policy() { with_bucket test_put_bucket_policy_impl; }
test_get_bucket_policy() { with_bucket test_get_bucket_policy_impl; }
test_delete_bucket_policy() { with_bucket test_delete_bucket_policy_impl; }
test_get_bucket_policy_status() { with_bucket test_get_bucket_policy_status_impl; }
test_get_bucket_policy_not_found() { with_bucket test_get_bucket_policy_not_found_impl; }

main() {
  printf 'Running bucket policy integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Put bucket policy (aws s3api put-bucket-policy)' test_put_bucket_policy
  run_test 'Get bucket policy (aws s3api get-bucket-policy)' test_get_bucket_policy
  run_test 'Delete bucket policy (aws s3api delete-bucket-policy)' test_delete_bucket_policy
  run_test 'Get bucket policy status (aws s3api get-bucket-policy-status)' test_get_bucket_policy_status
  run_test 'Get missing policy returns NoSuchBucketPolicy (aws s3api get-bucket-policy)' test_get_bucket_policy_not_found

  printf 'Policy tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
