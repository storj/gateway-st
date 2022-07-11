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
    umount "$MOUNT_DST_DIR"
    kill -SIGTERM "$S3FS_PID"
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

SRC_DIR=$TMPDIR/source
DST_DIR=$TMPDIR/dst
MOUNT_DST_DIR=$TMPDIR/mount-dst
mkdir -p "$SRC_DIR" "$DST_DIR" "$MOUNT_DST_DIR"

BUCKET="s3fs-bucket"

random_bytes_file () {
    count=$1
    blockSize=$2
    output=$3
    dd if=/dev/urandom of="$output" count="$count" bs="$blockSize" >/dev/null 2>&1
}

random_bytes_file 1  1024      "$SRC_DIR/small-upload-testfile"     # create 1kb file of random bytes (inline)
random_bytes_file 9  1024x1024 "$SRC_DIR/big-upload-testfile"       # create 9mb file of random bytes (remote)
# this is special case where we need to test at least one remote segment and inline segment of exact size 0
# value is invalid until we will be able to configure segment size once again
random_bytes_file 64 1024x1024 "$SRC_DIR/multipart-upload-testfile"

echo "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" > "$TMPDIR/.passwd-s3fs"
chmod 600 "$TMPDIR/.passwd-s3fs"

aws s3 --endpoint "$AWS_ENDPOINT" mb "s3://$BUCKET"

s3fs "$BUCKET" "$MOUNT_DST_DIR" -o passwd_file="$TMPDIR/.passwd-s3fs" -o url="$AWS_ENDPOINT" -o dbglevel=info -o use_path_request_style
S3FS_PID=$(pidof s3fs)

cp "$SRC_DIR/small-upload-testfile"     "$MOUNT_DST_DIR"
cp "$SRC_DIR/big-upload-testfile"       "$MOUNT_DST_DIR"
cp "$SRC_DIR/multipart-upload-testfile" "$MOUNT_DST_DIR"

cp "$MOUNT_DST_DIR/small-upload-testfile"     "$DST_DIR/small-upload-testfile"
cp "$MOUNT_DST_DIR/big-upload-testfile"       "$DST_DIR/big-upload-testfile"
cp "$MOUNT_DST_DIR/multipart-upload-testfile" "$DST_DIR/multipart-upload-testfile"

require_equal_files_content "$SRC_DIR/small-upload-testfile"     "$DST_DIR/small-upload-testfile"
require_equal_files_content "$SRC_DIR/big-upload-testfile"       "$DST_DIR/big-upload-testfile"
require_equal_files_content "$SRC_DIR/multipart-upload-testfile" "$DST_DIR/multipart-upload-testfile"

FILES=$(find "$MOUNT_DST_DIR" -maxdepth 1 -type f | wc -l)
EXPECTED_FILES="3"
if [ "$FILES" == "$EXPECTED_FILES" ]
then
    echo "listing returns $FILES files"
else
    echo "listing returns $FILES files but want $EXPECTED_FILES"
    exit 1
fi

FILES=$(aws s3 --endpoint "$AWS_ENDPOINT" ls "s3://$BUCKET" | wc -l)
EXPECTED_FILES="3"
if [ "$FILES" == "$EXPECTED_FILES" ]
then
    echo "listing returns $FILES files"
else
    echo "listing returns $FILES files but want $EXPECTED_FILES"
    exit 1
fi

rm "$MOUNT_DST_DIR/small-upload-testfile"
rm "$MOUNT_DST_DIR/big-upload-testfile"
rm "$MOUNT_DST_DIR/multipart-upload-testfile"

FILES=$(find "$MOUNT_DST_DIR" -maxdepth 1 -type f | wc -l)
EXPECTED_FILES="0"
if [ "$FILES" == "$EXPECTED_FILES" ]
then
    echo "listing returns $FILES files"
else
    echo "listing returns $FILES files but want $EXPECTED_FILES"
    exit 1
fi

FILES=$(aws s3 --endpoint "$AWS_ENDPOINT" ls "s3://$BUCKET" | wc -l)
EXPECTED_FILES="0"
if [ "$FILES" == "$EXPECTED_FILES" ]
then
    echo "listing returns $FILES files"
else
    echo "listing returns $FILES files but want $EXPECTED_FILES"
    exit 1
fi
