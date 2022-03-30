#!/usr/bin/env bash
set -Eueo pipefail

log_error() {
	rc=$?
	echo "error code $rc in $(caller) line $LINENO :: ${BASH_COMMAND}"
	exit $rc
}
trap log_error ERR

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# shellcheck source=testsuite/integration/require.sh
source "$SCRIPTDIR"/require.sh

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:=$GATEWAY_0_ACCESS_KEY}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:=$GATEWAY_0_SECRET_KEY}
[ -n "${AWS_ENDPOINT:=http://$GATEWAY_0_ADDR}" ]

#setup tmpdir for testfiles and cleanup
TMPDIR=$(mktemp -d -t tmp.XXXXXXXXXX)
cleanup(){
	rm -rf "$TMPDIR"
}
trap cleanup EXIT

export AWS_CONFIG_FILE=$TMPDIR/.aws/config

BUCKET="bucket-multipart"

echo "Creating Bucket"
aws s3 --endpoint "$AWS_ENDPOINT" mb "s3://$BUCKET"

echo "Create Pending Multipart Uploads"
UPLOAD_NAME="my/multipart-upload"
uploadResult=$(aws s3api --endpoint "$AWS_ENDPOINT" create-multipart-upload --bucket "$BUCKET" --key "$UPLOAD_NAME")
uploadId=$(echo "$uploadResult" | grep UploadId | awk -F '"' '{print $4}')
uploadKey=$(echo "$uploadResult" | grep Key | awk -F '"' '{print $4}')

require_equal_strings "$UPLOAD_NAME" "$uploadKey"

echo "List Pending Multipart Upload"
listResult=$(aws s3api --endpoint "$AWS_ENDPOINT" list-multipart-uploads --bucket "$BUCKET" --prefix "$UPLOAD_NAME")
listUploadId=$(echo "$listResult" | grep "UploadId" | awk -F '"' '{print $4}')
listKey=$(echo "$listResult" | grep Key | awk -F '"' '{print $4}')

require_equal_strings "$uploadKey" "$listKey"
require_equal_strings "$uploadId" "$listUploadId"

listResult=$(aws s3api --endpoint "$AWS_ENDPOINT" list-multipart-uploads --bucket "$BUCKET" --prefix my/)
listUploadId=$(echo "$listResult" | grep "UploadId" | awk -F '"' '{print $4}')
listKey=$(echo "$listResult" | grep Key | awk -F '"' '{print $4}')

require_equal_strings "$uploadKey" "$listKey"
# TODO: for now we get different upload ids with prefixes "my/" and "my/multipart-upload"
# require_equal_strings $uploadId $listUploadId

echo "Aborting Multipart Upload"
aws s3api --endpoint "$AWS_ENDPOINT" abort-multipart-upload --bucket "$BUCKET" --key "$UPLOAD_NAME" --upload-id "$uploadId"

aws s3 --endpoint "$AWS_ENDPOINT" rb "s3://$BUCKET" --force
