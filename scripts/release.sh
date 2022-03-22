#!/usr/bin/env bash

set -eu
set -o pipefail

echo -n "Build timestamp: "
TIMESTAMP=$(date +%s)
echo $TIMESTAMP

echo -n "Git commit: "
if [[ "$(git diff --stat)" != '' ]] || [[ -n "$(git status -s)" ]]; then
  COMMIT=$(git rev-parse HEAD)-dirty
  RELEASE=false
else
  COMMIT=$(git rev-parse HEAD)
  RELEASE=true
fi
echo $COMMIT

echo -n "Tagged version: "
if git describe --tags --exact-match --match "v[0-9]*.[0-9]*.[0-9]*"; then
  VERSION=$(git describe --tags --exact-match --match "v[0-9]*.[0-9]*.[0-9]*")
  echo $VERSION
else
  VERSION=v0.0.0
  RELEASE=false
fi

if [[ -v RELEASE_BUILD_REQUIRED ]] && $RELEASE_BUILD_REQUIRED && ! $RELEASE; then
  echo "ERROR: A release build is required, but a release build wasn't possible." 1>&2

  exit 1
fi

# minio needs an RFC 3339 or ISO 8601 formatted date/time set as the version
# otherwise the object browser breaks and refuses login.
# see https://github.com/storj/minio/blob/main/buildscripts/gen-ldflags.go
# for now, these are hardcoded as storj/minio isn't updated that often.
#
# TODO(artur, sean): this needs to be automated. Use
# storj.io/minio/buildscripts/gen-ldflags.go
MINIO_VERSION="2022-03-22T16:55:11Z"
MINIO_RELEASE="DEVELOPMENT.2022-03-22T16-55-11Z"
MINIO_COMMIT="d6f2ba63d1c637aafc4edf14dd538486a9197db2"
MINIO_SHORT_COMMIT="d6f2ba63d1c6"

echo Running "go $@"
exec go "$1" -ldflags \
	"-s -w -X storj.io/private/version.buildTimestamp=$TIMESTAMP
         -X storj.io/private/version.buildCommitHash=$COMMIT
         -X storj.io/private/version.buildVersion=$VERSION
         -X storj.io/private/version.buildRelease=$RELEASE
         -X storj.io/minio/cmd.Version=$MINIO_VERSION
         -X storj.io/minio/cmd.ReleaseTag=$MINIO_RELEASE
         -X storj.io/minio/cmd.CommitID=$MINIO_COMMIT
         -X storj.io/minio/cmd.ShortCommitID=$MINIO_SHORT_COMMIT" "${@:2}"
