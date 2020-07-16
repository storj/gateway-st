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
mkdir -p "$SRC_DIR" "$DST_DIR"

random_bytes_file () {
	size=$1
	output=$2
	dd if=/dev/urandom of="$output" count=1 bs="$size" >/dev/null 2>&1
}

random_bytes_file "1MiB"  "$SRC_DIR/backup-testfile-1MiB"  # create 1MiB file of random bytes (remote)
random_bytes_file "10MiB" "$SRC_DIR/backup-testfile-10MiB" # create 10MiB file of random bytes (remote)

BUCKET="backup-bucket"

duplicati-cli backup  s3://$BUCKET $SRC_DIR --s3-server-name=$GATEWAY_0_ADDR --aws_access_key_id=$GATEWAY_0_ACCESS_KEY --aws_secret_access_key=$GATEWAY_0_SECRET_KEY --passphrase=my-pass --use-ssl=false --debug-output=true

duplicati-cli restore s3://$BUCKET --restore-path=$DST_DIR --s3-server-name=$GATEWAY_0_ADDR --aws_access_key_id=$GATEWAY_0_ACCESS_KEY --aws_secret_access_key=$GATEWAY_0_SECRET_KEY --passphrase=my-pass --use-ssl=false --debug-output=true

require_equal_files_content "$SRC_DIR/backup-testfile-1MiB"  "$DST_DIR/backup-testfile-1MiB"
require_equal_files_content "$SRC_DIR/backup-testfile-10MiB" "$DST_DIR/backup-testfile-10MiB"