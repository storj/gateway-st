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

SRC_DIR=$TMPDIR/source
DST_DIR=$TMPDIR/dst
SYNC_DST_DIR=$TMPDIR/sync-dst
mkdir -p "$SRC_DIR" "$DST_DIR" "$SYNC_DST_DIR"


export AWS_CONFIG_FILE=$TMPDIR/.aws/config
export AWS_SHARED_CREDENTIALS_FILE=$TMPDIR/.aws/credentials

aws configure set aws_access_key_id     "$GATEWAY_0_ACCESS_KEY"
aws configure set aws_secret_access_key "$GATEWAY_0_SECRET_KEY"
aws configure set default.region        us-east-1

random_bytes_file () {
	count=$1
    size=$2
	output=$3
	dd if=/dev/urandom of="$output" count=$count bs="$size" >/dev/null 2>&1
}

random_bytes_file 1  1024      "$SRC_DIR/small-upload-testfile"     # create 1kb file of random bytes (inline)
random_bytes_file 9  1024x1024 "$SRC_DIR/big-upload-testfile"       # create 9mb file of random bytes (remote)
# this is special case where we need to test at least one remote segment and inline segment of exact size 0
# value is invalid until we will be able to configure segment size once again
random_bytes_file 64 1024x1024 "$SRC_DIR/multipart-upload-testfile"

echo "Creating Bucket"
aws s3 --endpoint="http://$GATEWAY_0_ADDR" mb s3://bucket

echo "Uploading Files"
aws configure set default.s3.multipart_threshold 1TB
aws s3 --endpoint="http://$GATEWAY_0_ADDR" --no-progress cp "$SRC_DIR/small-upload-testfile" s3://bucket/small-testfile
aws s3 --endpoint="http://$GATEWAY_0_ADDR" --no-progress cp "$SRC_DIR/big-upload-testfile"   s3://bucket/big-testfile

# Wait 5 seconds to trigger any error related to one of the different intervals
sleep 5

echo "Uploading Multipart File"
aws configure set default.s3.multipart_threshold 4KB
aws s3 --endpoint="http://$GATEWAY_0_ADDR" --no-progress cp "$SRC_DIR/multipart-upload-testfile" s3://bucket/multipart-testfile

echo "Downloading Files"
aws s3 --endpoint="http://$GATEWAY_0_ADDR" ls s3://bucket
aws s3 --endpoint="http://$GATEWAY_0_ADDR" --no-progress cp s3://bucket/small-testfile     "$DST_DIR/small-download-testfile"
aws s3 --endpoint="http://$GATEWAY_0_ADDR" --no-progress cp s3://bucket/big-testfile       "$DST_DIR/big-download-testfile"
aws s3 --endpoint="http://$GATEWAY_0_ADDR" --no-progress cp s3://bucket/multipart-testfile "$DST_DIR/multipart-download-testfile"
aws s3 --endpoint="http://$GATEWAY_0_ADDR" rb s3://bucket --force

require_equal_files_content "$SRC_DIR/small-upload-testfile"     "$DST_DIR/small-download-testfile"
require_equal_files_content "$SRC_DIR/big-upload-testfile"       "$DST_DIR/big-download-testfile"
require_equal_files_content "$SRC_DIR/multipart-upload-testfile" "$DST_DIR/multipart-download-testfile"

echo "Creating Bucket for sync test"
aws s3 --endpoint="http://$GATEWAY_0_ADDR" mb s3://bucket-sync

echo "Sync Files"
aws s3 --endpoint="http://$GATEWAY_0_ADDR" --no-progress sync "$SRC_DIR" s3://bucket-sync
aws s3 --endpoint="http://$GATEWAY_0_ADDR" --no-progress sync s3://bucket-sync "$SYNC_DST_DIR"

aws s3 --endpoint="http://$GATEWAY_0_ADDR" rb s3://bucket-sync --force

echo "Compare sync directories"
diff "$SRC_DIR" "$SYNC_DST_DIR"
