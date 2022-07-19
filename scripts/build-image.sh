#!/usr/bin/env bash
set -euo pipefail

BUILD_NUMBER=${1:-}
GO_VERSION=${2:-}

if [ -z "$BUILD_NUMBER" ]; then
	echo "Missing first arg build number, e.g. 123"
	exit 2
fi
if [ -z "$GO_VERSION" ]; then
	echo "Missing second arg Go version, e.g. 1.17.5"
	exit 2
fi

if go env > /dev/null; then
	PKG_CACHE_PATH=$(go env GOPATH)
	GOARCH=$(go env GOARCH)
else
	# go is not installed, make a few assumptions about environment
	mkdir -p /tmp/go-pkg
	PKG_CACHE_PATH=/tmp/go-pkg
	GOARCH=amd64
fi

case $GOARCH in
	arm)   DOCKER_ARCH=arm32v6 ;;
	arm64) DOCKER_ARCH=arm64v8 ;;
	*)     DOCKER_ARCH=amd64   ;;
esac

docker run \
	-u "$(id -u)":"$(id -g)" \
	-v "$PWD":/go/build \
	-v "$PKG_CACHE_PATH":/go/pkg \
	-e GOARM=6 -e GOOS=linux -e GOARCH="$GOARCH" -e GOPROXY \
	-w /go/build \
	--rm storjlabs/golang:"$GO_VERSION" \
	go build -o release/"$BUILD_NUMBER"/gateway_linux_"$GOARCH" \
		storj.io/gateway

trap 'rm -r release/"$BUILD_NUMBER"' EXIT

docker build \
	--build-arg TAG="$BUILD_NUMBER" \
	--build-arg GOARCH="$GOARCH" \
	--build-arg DOCKER_ARCH="$DOCKER_ARCH" \
	--label build="$BUILD_NUMBER" \
	-t storjlabs/gateway:"$BUILD_NUMBER" .
