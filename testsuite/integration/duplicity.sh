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

SRC_DIR=$TMPDIR/source
DST_DIR=$TMPDIR/dst
DST_DIR_MULTIPART=$TMPDIR/dst-multipart

mkdir -p "$SRC_DIR" "$DST_DIR" "$DST_DIR_MULTIPART"

random_bytes_file () {
	size=$1
	output=$2
	dd if=/dev/urandom of="$output" count=1 bs="$size" >/dev/null 2>&1
}

random_bytes_file "1MiB"  "$SRC_DIR/backup-testfile-1MiB"  # create 1MiB file of random bytes (remote)
random_bytes_file "10MiB" "$SRC_DIR/backup-testfile-10MiB" # create 1-MiB file of random bytes (remote)

export PASSPHRASE="PASSPHRASE"

# duplicity 0.8.x w/ boto3 doesn't seem to create buckets anymore
aws s3 --endpoint "$AWS_ENDPOINT" mb s3://duplicity
aws s3 --endpoint "$AWS_ENDPOINT" mb s3://duplicity-multipart

duplicity -v9 --s3-endpoint-url="$AWS_ENDPOINT" "$SRC_DIR" "boto3+s3://duplicity/"
duplicity -v9 --s3-endpoint-url="$AWS_ENDPOINT" "boto3+s3://duplicity/" "$DST_DIR"

require_equal_files_content "$SRC_DIR/backup-testfile-1MiB"  "$DST_DIR/backup-testfile-1MiB"
require_equal_files_content "$SRC_DIR/backup-testfile-10MiB" "$DST_DIR/backup-testfile-10MiB"

# use multipart upload
duplicity -v9 --s3-endpoint-url="$AWS_ENDPOINT" "$SRC_DIR" \
	"boto3+s3://duplicity-multipart/" \
	--s3-use-multiprocessing \
	--s3-multipart-max-procs 2 \
	--s3-multipart-chunk-size 2097152

duplicity -v9 --s3-endpoint-url="$AWS_ENDPOINT" "boto3+s3://duplicity-multipart/" "$DST_DIR_MULTIPART"

require_equal_files_content "$SRC_DIR/backup-testfile-1MiB"  "$DST_DIR_MULTIPART/backup-testfile-1MiB"
require_equal_files_content "$SRC_DIR/backup-testfile-10MiB" "$DST_DIR_MULTIPART/backup-testfile-10MiB"
