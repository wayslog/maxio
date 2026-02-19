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
  printf 'maxio-it-multipart-%s-%s' "$(date +%s)" "$RANDOM"
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

test_create_upload_and_upload_part_impl() {
  local bucket="$1"
  local part_file="$SCRIPT_DIR/.tmp-mpu-part-$RANDOM.bin"
  local upload_id

  dd if=/dev/zero of="$part_file" bs=1048576 count=1 >/dev/null 2>&1

  upload_id="$(aws s3api create-multipart-upload --bucket "$bucket" --key 'multipart.bin' --query 'UploadId' --output text "${AWS_ARGS[@]}")"
  aws s3api upload-part --bucket "$bucket" --key 'multipart.bin' --part-number 1 --upload-id "$upload_id" --body "$part_file" "${AWS_ARGS[@]}" >/dev/null

  aws s3api abort-multipart-upload --bucket "$bucket" --key 'multipart.bin' --upload-id "$upload_id" "${AWS_ARGS[@]}" >/dev/null
  rm -f "$part_file"
}

test_complete_multipart_upload_impl() {
  local bucket="$1"
  local part_file="$SCRIPT_DIR/.tmp-mpu-complete-part-$RANDOM.bin"
  local complete_json="$SCRIPT_DIR/.tmp-mpu-complete-$RANDOM.json"
  local upload_id etag

  dd if=/dev/zero of="$part_file" bs=1048576 count=1 >/dev/null 2>&1
  upload_id="$(aws s3api create-multipart-upload --bucket "$bucket" --key 'complete.bin' --query 'UploadId' --output text "${AWS_ARGS[@]}")"
  etag="$(aws s3api upload-part --bucket "$bucket" --key 'complete.bin' --part-number 1 --upload-id "$upload_id" --body "$part_file" --query 'ETag' --output text "${AWS_ARGS[@]}")"

  cat >"$complete_json" <<EOF
{
  "Parts": [
    {
      "ETag": $etag,
      "PartNumber": 1
    }
  ]
}
EOF

  aws s3api complete-multipart-upload --bucket "$bucket" --key 'complete.bin' --upload-id "$upload_id" --multipart-upload "file://$complete_json" "${AWS_ARGS[@]}" >/dev/null
  aws s3api head-object --bucket "$bucket" --key 'complete.bin' "${AWS_ARGS[@]}" >/dev/null

  rm -f "$part_file" "$complete_json"
}

test_abort_multipart_upload_impl() {
  local bucket="$1"
  local part_file="$SCRIPT_DIR/.tmp-mpu-abort-part-$RANDOM.bin"
  local upload_id

  dd if=/dev/zero of="$part_file" bs=1048576 count=1 >/dev/null 2>&1
  upload_id="$(aws s3api create-multipart-upload --bucket "$bucket" --key 'abort.bin' --query 'UploadId' --output text "${AWS_ARGS[@]}")"
  aws s3api upload-part --bucket "$bucket" --key 'abort.bin' --part-number 1 --upload-id "$upload_id" --body "$part_file" "${AWS_ARGS[@]}" >/dev/null
  aws s3api abort-multipart-upload --bucket "$bucket" --key 'abort.bin' --upload-id "$upload_id" "${AWS_ARGS[@]}" >/dev/null

  if aws s3api list-multipart-uploads --bucket "$bucket" --query "Uploads[?UploadId=='$upload_id'].UploadId" --output text "${AWS_ARGS[@]}" | grep -q "$upload_id"; then
    rm -f "$part_file"
    return 1
  fi

  rm -f "$part_file"
}

test_list_multipart_uploads_impl() {
  local bucket="$1"
  local upload_id

  upload_id="$(aws s3api create-multipart-upload --bucket "$bucket" --key 'list.bin' --query 'UploadId' --output text "${AWS_ARGS[@]}")"

  aws s3api list-multipart-uploads --bucket "$bucket" --query "Uploads[?UploadId=='$upload_id'].UploadId" --output text "${AWS_ARGS[@]}" | grep -q "$upload_id"
  aws s3api abort-multipart-upload --bucket "$bucket" --key 'list.bin' --upload-id "$upload_id" "${AWS_ARGS[@]}" >/dev/null
}

test_large_file_upload_via_cp_impl() {
  local bucket="$1"
  local large_file="$SCRIPT_DIR/.tmp-large-$RANDOM.bin"

  dd if=/dev/zero of="$large_file" bs=1048576 count=20 >/dev/null 2>&1
  aws s3 cp "$large_file" "s3://$bucket/large.bin" "${AWS_ARGS[@]}" >/dev/null
  aws s3api head-object --bucket "$bucket" --key 'large.bin' "${AWS_ARGS[@]}" >/dev/null

  rm -f "$large_file"
}

test_create_upload_and_upload_part() { with_bucket test_create_upload_and_upload_part_impl; }
test_complete_multipart_upload() { with_bucket test_complete_multipart_upload_impl; }
test_abort_multipart_upload() { with_bucket test_abort_multipart_upload_impl; }
test_list_multipart_uploads() { with_bucket test_list_multipart_uploads_impl; }
test_large_file_upload_via_cp() { with_bucket test_large_file_upload_via_cp_impl; }

main() {
  printf 'Running multipart integration tests against %s\n' "$S3_ENDPOINT_URL"

  run_test 'Create multipart upload + upload part' test_create_upload_and_upload_part
  run_test 'Complete multipart upload' test_complete_multipart_upload
  run_test 'Abort multipart upload' test_abort_multipart_upload
  run_test 'List multipart uploads' test_list_multipart_uploads
  run_test 'Large file upload via aws s3 cp' test_large_file_upload_via_cp

  printf 'Multipart tests complete: %s passed, %s failed\n' "$PASS_COUNT" "$FAIL_COUNT"
  [ "$FAIL_COUNT" -eq 0 ]
}

main "$@"
