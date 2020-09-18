#!/usr/bin/env bash
set -ueo pipefail
set +x

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

service postgresql start
cd $SCRIPTDIR && go install \
	storj.io/storj/cmd/certificates \
	storj.io/storj/cmd/identity \
	storj.io/storj/cmd/satellite \
	storj.io/storj/cmd/storagenode \
	storj.io/storj/cmd/versioncontrol \
	storj.io/storj/cmd/storj-sim

cd $SCRIPTDIR && go install storj.io/gateway

until pg_isready; do sleep 3; done

psql -U postgres -c 'create database teststorj4;'

export STORJ_SIM_POSTGRES=postgres://postgres@localhost/teststorj4?sslmode=disable

# setup the network
storj-sim -x network destroy
storj-sim -x  --host 0.0.0.0 --satellites 1 network --dev --postgres=$STORJ_SIM_POSTGRES setup

sed -i 's/# metainfo.rate-limiter.enabled: true/metainfo.rate-limiter.enabled: false/g' $(storj-sim network env SATELLITE_0_DIR)/config.yaml
storj-sim -x --host 0.0.0.0 --satellites 1 network --dev run
