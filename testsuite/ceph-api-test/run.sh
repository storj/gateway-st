#!/usr/bin/env bash
set -Eueo pipefail

# use 'make integration-ceph-api-tests' to run this script

# Runs ceph's s3-tests against the api package over the filesystem object layer
# (api/internal/fsgateway), so that no satellite is needed. $FSGATEWAY is the fsgateway binary.

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BUILD_DIR="$SCRIPTDIR/../../.build"
FSGATEWAY=${FSGATEWAY:-$BUILD_DIR/fsgateway}

# Only the report and the log go to the bind-mounted build directory, which is slow to write
# many files to and owned by another user under some Docker setups.
WORK_DIR=$(mktemp -d)

# The credentials match storj.conf.
AWS_ACCESS_KEY_ID=fsaccess AWS_SECRET_ACCESS_KEY=fssecret \
ALT_AWS_ACCESS_KEY_ID=fsalt ALT_AWS_SECRET_ACCESS_KEY=fsaltsecret \
	"$FSGATEWAY" -address 127.0.0.1:7777 -dir "$WORK_DIR/data" > "$BUILD_DIR/fsgateway.log" 2>&1 &
FSGATEWAY_PID=$!
trap 'kill "$FSGATEWAY_PID"' EXIT

pushd "$WORK_DIR"
    # s3-tests lives in ceph/ceph, so only its directory is fetched. Fetching by hash pins the
    # content.
    git init -q s3-tests
    cd s3-tests
    git remote add origin https://github.com/ceph/ceph.git
    git sparse-checkout set --no-cone /src/test/rgw/s3-tests/
    git fetch -q --depth 1 --filter=blob:none origin ecfe96faab9a46d00ab3c2e74897c8656c28e1a1
    git checkout -q FETCH_HEAD
    cd src/test/rgw/s3-tests

    python -m venv .venv
    .venv/bin/pip install -q -r "$SCRIPTDIR/requirements.txt"

    # all.tests lists every test once: enabled as is, or disabled as "#(reason) test".
    if grep -vnE '^(#\([^)]+\) )?s3tests/' "$SCRIPTDIR/all.tests"; then
        echo "all.tests: the lines above are neither 'test' nor '#(reason) test'"
        exit 1
    fi
    if ! diff -u \
        <(sed -E 's/^#\([^)]+\) //' "$SCRIPTDIR/all.tests" | sort) \
        <(S3TEST_CONF="$SCRIPTDIR/storj.conf" .venv/bin/pytest -q --collect-only -p no:cacheprovider s3tests | grep '::' | sed 's/\[.*//' | sort -u); then
        echo "all.tests: add the tests marked + and remove the ones marked -"
        exit 1
    fi

    # Wait for fsgateway; any response, even an error, means it's serving.
    curl -s --retry 10 --retry-connrefused --retry-delay 1 -o /dev/null http://127.0.0.1:7777/

    #shellcheck disable=SC2046
    S3TEST_CONF="$SCRIPTDIR/storj.conf" .venv/bin/pytest -n 4 $( grep "^[^#]" "$SCRIPTDIR/all.tests" | xargs) --junitxml="$BUILD_DIR/ceph-api.xml"
popd
