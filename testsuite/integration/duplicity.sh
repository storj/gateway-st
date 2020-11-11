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
DST_DIR_MULTIPART=$TMPDIR/dst-multipart
mkdir -p "$SRC_DIR" "$DST_DIR" "$DST_DIR_MULTIPART"

random_bytes_file () {
	size=$1
	output=$2
	dd if=/dev/urandom of="$output" count=1 bs="$size" >/dev/null 2>&1
}

random_bytes_file "1MiB"  "$SRC_DIR/backup-testfile-1MiB"  # create 1MiB file of random bytes (remote)
random_bytes_file "10MiB" "$SRC_DIR/backup-testfile-10MiB" # create 1-MiB file of random bytes (remote)

export AWS_ACCESS_KEY_ID=$GATEWAY_0_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$GATEWAY_0_SECRET_KEY
export PASSPHRASE="PASSPHRASE"

duplicity -v9 $SRC_DIR s3://$GATEWAY_0_ADDR/duplicity/ --s3-unencrypted-connection

duplicity -v9 s3://$GATEWAY_0_ADDR/duplicity/ $DST_DIR --s3-unencrypted-connection

require_equal_files_content "$SRC_DIR/backup-testfile-1MiB"  "$DST_DIR/backup-testfile-1MiB"
require_equal_files_content "$SRC_DIR/backup-testfile-10MiB" "$DST_DIR/backup-testfile-10MiB"

# TODO: below test fails because listing multiparts uploads returns different upload IDs every time.
# use multipart upload
#duplicity -v9 $SRC_DIR s3://$GATEWAY_0_ADDR/duplicity-multipart/ --s3-unencrypted-connection --s3-use-multiprocessing --s3-multipart-max-procs 2 --s3-multipart-chunk-size 2097152

#duplicity -v9 s3://$GATEWAY_0_ADDR/duplicity-multipart/ $DST_DIR_MULTIPART --s3-unencrypted-connection

#require_equal_files_content "$SRC_DIR/backup-testfile-1MiB"  "$DST_DIR_MULTIPART/backup-testfile-1MiB"
#require_equal_files_content "$SRC_DIR/backup-testfile-10MiB" "$DST_DIR_MULTIPART/backup-testfile-10MiB"