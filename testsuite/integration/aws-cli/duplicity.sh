#!/usr/bin/env bash
set -ueo pipefail

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
random_bytes_file "10MiB" "$SRC_DIR/backup-testfile-10MiB" # create 1-MiB file of random bytes (remote)

export AWS_ACCESS_KEY_ID=$GATEWAY_0_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$GATEWAY_0_SECRET_KEY
export PASSPHRASE="PASSPHRASE"

duplicity -v 9 $SRC_DIR s3://$GATEWAY_0_ADDR/duplicity/ --s3-unencrypted-connection

duplicity -v 9 s3://$GATEWAY_0_ADDR/duplicity/ $DST_DIR --s3-unencrypted-connection

if cmp "$SRC_DIR/backup-testfile-1MiB" "$DST_DIR/backup-testfile-1MiB"
then
	echo "backup-testfile-1MiB file matches backup file";
else
	echo "backup-testfile-1MiB file does not match backup file";
    exit 1
fi

if cmp "$SRC_DIR/backup-testfile-10MiB" "$DST_DIR/backup-testfile-10MiB"
then
	echo "backup-testfile-10MiB file matches backup file";
else
	echo "backup-testfile-10MiB file does not match backup file";
    exit 1
fi

