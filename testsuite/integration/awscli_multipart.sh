#!/usr/bin/env bash
set -ueo pipefail

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $SCRIPTDIR/require.sh

#setup tmpdir for testfiles and cleanup
TMPDIR=$(mktemp -d -t tmp.XXXXXXXXXX)
cleanup(){
	rm -rf "$TMPDIR"
}
trap cleanup EXIT

export AWS_CONFIG_FILE=$TMPDIR/.aws/config
export AWS_SHARED_CREDENTIALS_FILE=$TMPDIR/.aws/credentials

aws configure set aws_access_key_id     "$GATEWAY_0_ACCESS_KEY"
aws configure set aws_secret_access_key "$GATEWAY_0_SECRET_KEY"
aws configure set default.region        us-east-1

echo "Creating Bucket"
aws s3 --endpoint="http://$GATEWAY_0_ADDR" mb s3://bucket


echo "Create Pending Multipart Uploads"
UPLOAD_NAME="my/multipart-upload"
uploadResult=`aws s3api --endpoint="http://$GATEWAY_0_ADDR" create-multipart-upload --bucket bucket --key $UPLOAD_NAME`
uploadId=`echo "$uploadResult" | grep UploadId | awk -F '"' '{print $4}'`
uploadKey=`echo "$uploadResult" | grep Key | awk -F '"' '{print $4}'`

require_equal_strings $UPLOAD_NAME $uploadKey

echo "List Pending Multipart Upload"
listResult=`aws s3api --endpoint="http://$GATEWAY_0_ADDR" list-multipart-uploads --bucket bucket --prefix $UPLOAD_NAME`
listUploadId=`echo "$listResult" | grep "UploadId" | awk -F '"' '{print $4}'`
listKey=`echo "$listResult" | grep Key | awk -F '"' '{print $4}'`

require_equal_strings $uploadKey $listKey
require_equal_strings $uploadId $listUploadId

listResult=`aws s3api --endpoint="http://$GATEWAY_0_ADDR" list-multipart-uploads --bucket bucket --prefix my/`
listUploadId=`echo "$listResult" | grep "UploadId" | awk -F '"' '{print $4}'`
listKey=`echo "$listResult" | grep Key | awk -F '"' '{print $4}'`

require_equal_strings $uploadKey $listKey
# TODO: for now we get different upload ids with prefixes "my/" and "my/multipart-upload"
# require_equal_strings $uploadId $listUploadId

echo "Aborting Multipart Upload"
aws s3api --endpoint="http://$GATEWAY_0_ADDR" abort-multipart-upload --bucket bucket --key "$UPLOAD_NAME" --upload-id "$uploadId"

aws s3 --endpoint="http://$GATEWAY_0_ADDR" rb s3://bucket --force
