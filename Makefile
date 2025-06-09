#
# Common
#

.PHONY: help
help:
	@awk 'BEGIN { \
		FS = ":.*##"; \
		printf "\nUsage:\n  make \033[36m<target>\033[0m\n" \
	} \
	/^[a-zA-Z_-]+:.*?##/ { \
		printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2 \
	} \
	/^##@/ { \
		printf "\n\033[1m%s\033[0m\n", substr($$0, 5) \
	}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

#
# Public Jenkins (commands below are used for local development and/or public Jenkins)
#

##@ Local development/Public Jenkins/Helpers

.PHONY: install-dev-dependencies
install-dev-dependencies: ## install-dev-dependencies assumes Go and cURL are installed
	# Storj-specific:
	go install github.com/storj/ci/check-mod-tidy@latest
	go install github.com/storj/ci/check-copyright@latest
	go install github.com/storj/ci/check-large-files@latest
	go install github.com/storj/ci/check-imports@latest
	go install github.com/storj/ci/check-peer-constraints@latest
	go install github.com/storj/ci/check-atomic-align@latest
	go install github.com/storj/ci/check-monkit@latest
	go install github.com/storj/ci/check-errs@latest
	go install github.com/storj/ci/check-downgrades@latest
	go install github.com/storj/ci/storj-release@latest

	# staticcheck:
	go install honnef.co/go/tools/cmd/staticcheck@latest

	# golangci-lint:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.57.0

	# shellcheck (TODO(artur): Windows)
ifneq ($(shell which apt-get),)
	sudo apt-get install -y shellcheck
else ifneq ($(shell which brew),)
	brew install shellcheck
else
	$(error Can't install shellcheck without a supported package manager)
endif

.PHONY: bump-code-dependencies
bump-code-dependencies:
	go get storj.io/minio@main   && go mod tidy && cd testsuite && go mod tidy && cd .. && \
	go get storj.io/common@main  && go mod tidy && cd testsuite && go mod tidy && cd .. && \
	go get storj.io/uplink@main  && go mod tidy && cd testsuite && go mod tidy && \
	go get storj.io/storj@latest && go mod tidy

.PHONY: install-hooks
install-hooks: ## Install helpful Git hooks
	ln -s ../../githooks/pre-commit .git/hooks/pre-commit

##@ Local development/Public Jenkins/Lint

GOLANGCI_LINT_CONFIG ?= ../ci/.golangci.yml
GOLANGCI_LINT_CONFIG_TESTSUITE ?= ../../ci/.golangci.yml

.PHONY: lint
lint: ## Lint
	check-mod-tidy
	check-copyright
	check-large-files
	check-imports -race ./...
	check-peer-constraints -race
	check-atomic-align ./...
	check-monkit ./...
	check-errs ./...
	staticcheck ./...
	golangci-lint run --print-resources-usage --config ${GOLANGCI_LINT_CONFIG}
	check-downgrades

	# A bit of an explanation around this shellcheck command:
	# * Find all scripts recursively that have the .sh extension, except for "testsuite@tmp" which Jenkins creates temporarily
	# * Use + instead of \ so find returns a non-zero exit if any invocation of shellcheck returns a non-zero exit
	find . -path ./testsuite@tmp -prune -o -name "*.sh" -type f -exec "shellcheck" "-x" "--format=gcc" {} +;

	# Execute lint-testsuite in testsuite directory:
	$(MAKE) -C testsuite -f ../Makefile lint-testsuite

.PHONY: lint-testsuite
lint-testsuite: ## Lint testsuite
	check-imports -race ./...
	check-atomic-align ./...
	check-monkit ./...
	check-errs ./...
	staticcheck ./...
	golangci-lint run --print-resources-usage --config ${GOLANGCI_LINT_CONFIG_TESTSUITE}

##@ Local development/Public Jenkins/Cross-Vet

.PHONY: cross-vet
cross-vet: ## Cross-Vet
	GOOS=linux   GOARCH=386   go vet ./...
	GOOS=linux   GOARCH=amd64 go vet ./...
	GOOS=linux   GOARCH=arm   go vet ./...
	GOOS=linux   GOARCH=arm64 go vet ./...
	GOOS=freebsd GOARCH=386   go vet ./...
	GOOS=freebsd GOARCH=amd64 go vet ./...
	GOOS=freebsd GOARCH=arm   go vet ./...
	GOOS=freebsd GOARCH=arm64 go vet ./...
	GOOS=windows GOARCH=386   go vet ./...
	GOOS=windows GOARCH=amd64 go vet ./...

	# TODO(artur): find out if we will be able to enable these:
	# GOOS=windows GOARCH=arm   go vet ./...
	# GOOS=windows GOARCH=arm64 go vet ./...

	# Use kqueue to avoid using Cgo for verification.
	GOOS=darwin GOARCH=amd64 go vet -tags kqueue ./...
	GOOS=darwin GOARCH=arm64 go vet -tags kqueue ./...

##@ Local development/Public Jenkins/Test

JSON ?= false
SHORT ?= true
SKIP_TESTSUITE ?= false

.PHONY: test
test: test-testsuite ## Test
	go test -json=${JSON} -p 16 -parallel 4 -race -short=${SHORT} -timeout 10m -vet=off ./...

.PHONY: test-testsuite
test-testsuite: ## Test testsuite
ifeq (${SKIP_TESTSUITE},false)
	# Execute test-testsuite-do in testsuite directory:
	$(MAKE) -C testsuite -f ../Makefile test-testsuite-do
endif

.PHONY: test-testsuite-do
test-testsuite-do:
	go vet ./...
	go test -json=${JSON} -p 16 -parallel 4 -race -short=${SHORT} -timeout 10m -vet=off ./...

##@ Local development/Public Jenkins/Verification

.PHONY: verify
verify: lint cross-vet test ## Execute pre-commit verification

#
# Private Jenkins (commands below are used for releases/private Jenkins)
#

##@ Release/Private Jenkins/Build

GO_VERSION ?= 1.24.2
BRANCH_NAME ?= $(shell git rev-parse --abbrev-ref HEAD | sed "s!/!-!g")

ifeq (${BRANCH_NAME},main)
	TAG := $(shell git rev-parse --short HEAD)-go${GO_VERSION}
	BRANCH_NAME :=
else
	TAG := $(shell git rev-parse --short HEAD)-${BRANCH_NAME}-go${GO_VERSION}
	ifneq ($(shell git describe --tags --exact-match --match "v[0-9]*\.[0-9]*\.[0-9]*"),)
		LATEST_STABLE_TAG := latest
	endif
endif

DOCKER_BUILD := docker build --build-arg TAG=${TAG}

LATEST_DEV_TAG := dev

.PHONY: images
images: ## Build Docker images
	${DOCKER_BUILD} --platform linux/amd64 --pull=true -t storjlabs/gateway:${TAG}-amd64 \
		-f Dockerfile .
	${DOCKER_BUILD} --platform linux/arm/v6 --pull=true -t storjlabs/gateway:${TAG}-arm32v6 \
		--build-arg=GOARCH=arm \
		--build-arg=DOCKER_ARCH=arm32v6 \
		-f Dockerfile .
	${DOCKER_BUILD} --platform linux/arm64 --pull=true -t storjlabs/gateway:${TAG}-arm64v8 \
		--build-arg=GOARCH=arm64 \
		--build-arg=DOCKER_ARCH=arm64v8 \
		-f Dockerfile .

	docker tag storjlabs/gateway:${TAG}-amd64 storjlabs/gateway:${LATEST_DEV_TAG}

	@echo Built version: ${TAG}

# TODO(artur, sean): these extra ldflags are required for the minio object
# browser to function, but should be automated. Use storj.io/minio/buildscripts/gen-ldflags.go
LDFLAGS := -X storj.io/minio/cmd.Version=2022-04-19T11:13:21Z \
	-X storj.io/minio/cmd.ReleaseTag=DEVELOPMENT.2022-04-19T11-13-21Z \
	-X storj.io/minio/cmd.CommitID=ae15cc41053bb0a65e543f71d81646dd2318fe10 \
	-X storj.io/minio/cmd.ShortCommitID=ae15cc41053b

.PHONY: binaries
binaries: ## Build binaries
	CGO_ENABLED=0 storj-release \
		--build-name gateway \
		--build-tags kqueue \
		--ldflags "${LDFLAGS}" \
		--go-version "${GO_VERSION}" \
		--branch "${BRANCH_NAME}" \
		--skip-osarches "freebsd/amd64"

	# freebsd/amd64 requires CGO_ENABLED=1: https://github.com/storj/gateway-st/issues/62
	CGO_ENABLED=1 storj-release \
		--build-name gateway \
		--build-tags kqueue \
		--ldflags "${LDFLAGS}" \
		--go-version "${GO_VERSION}" \
		--branch "${BRANCH_NAME}" \
		--osarches "freebsd/amd64"

.PHONY: push-images
push-images: ## Push Docker images to Docker Hub
	# images have to be pushed before a manifest can be created
	for c in gateway; do \
		docker push storjlabs/$$c:${TAG}-amd64 \
		&& docker push storjlabs/$$c:${TAG}-arm32v6 \
		&& docker push storjlabs/$$c:${TAG}-arm64v8 \
		&& for t in ${TAG} ${LATEST_DEV_TAG} ${LATEST_STABLE_TAG}; do \
			docker manifest create storjlabs/$$c:$$t \
			storjlabs/$$c:${TAG}-amd64 \
			storjlabs/$$c:${TAG}-arm32v6 \
			storjlabs/$$c:${TAG}-arm64v8 \
			&& docker manifest annotate storjlabs/$$c:$$t storjlabs/$$c:${TAG}-amd64 --os linux --arch amd64 \
			&& docker manifest annotate storjlabs/$$c:$$t storjlabs/$$c:${TAG}-arm32v6 --os linux --arch arm --variant v6 \
			&& docker manifest annotate storjlabs/$$c:$$t storjlabs/$$c:${TAG}-arm64v8 --os linux --arch arm64 --variant v8 \
			&& docker manifest push --purge storjlabs/$$c:$$t \
		; done \
	; done

.PHONY: binaries-upload
binaries-upload: ## Upload release binaries to GCS
	cd "release/${TAG}"; for f in *; do \
		c="$${f%%_*}" \
		&& if [ "$${f##*.}" != "$${f}" ]; then \
			ln -s "$${f}" "$${f%%_*}.$${f##*.}" \
			&& zip "$${f}.zip" "$${f%%_*}.$${f##*.}" \
			&& rm "$${f%%_*}.$${f##*.}" \
		; else \
			ln -sf "$${f}" "$${f%%_*}" \
			&& zip "$${f}.zip" "$${f%%_*}" \
			&& rm "$${f%%_*}" \
		; fi \
	; done
	cd "release/${TAG}" \
		&& sha256sum *.zip > sha256sums \
		&& gsutil -m cp -r *.zip sha256sums "gs://storj-v3-alpha-builds/${TAG}/"

##@ Release/Private Jenkins/Clean

.PHONY: clean
clean: clean-binaries clean-images ## Remove local release binaries and local Docker images

.PHONY: clean-binaries
clean-binaries: ## Remove local release binaries
	rm -rf release

.PHONY: clean-images
clean-images:
	-docker rmi storjlabs/gateway:${TAG}

##@ Local development/Public Jenkins/Integration Test

BUILD_NUMBER ?= ${TAG}

.PHONY: integration-run
integration-run: integration-env-start integration-all-tests ## Start the integration environment and run all tests

.PHONY: integration-env-start
integration-env-start: integration-image-build integration-network-create integration-services-start ## Start the integration environment

.PHONY: integration-env-stop
integration-env-stop: ## Stop all running services in the integration environment
	-docker stop --time=1 $$(docker ps -qf network=integration-network-${BUILD_NUMBER})

.PHONY: integration-env-clean
integration-env-clean:
	-docker rm $$(docker ps -aqf network=integration-network-${BUILD_NUMBER})
	-docker rmi $$(docker image ls -qf label=build=${BUILD_NUMBER})
	-docker rmi redis:latest
	-docker rmi postgres:latest
	-docker rmi storjlabs/gateway-mint:latest
	-docker rmi storjlabs/splunk-s3-tests:latest
	-docker compose down
	-rm -rf storj
	-rm -rf edge.Dockerfile storj.Dockerfile docker-compose.yaml

.PHONY: integration-env-purge
integration-env-purge: integration-env-stop integration-env-clean integration-network-remove ## Purge the integration environment

.PHONY: integration-env-logs
integration-env-logs: ## Retrieve logs from integration services
	-docker logs integration-sim-${BUILD_NUMBER}
	-docker logs integration-gateway-${BUILD_NUMBER}

.PHONY: integration-all-tests
integration-all-tests: integration-gateway-st-tests integration-mint-tests integration-splunk-tests ## Run all integration tests (environment needs to be started first)

# note: umask 0000 is needed for rclone tests so files can be cleaned up.
.PHONY: integration-gateway-st-tests
integration-gateway-st-tests: ## Run gateway-st test suite (environment needs to be started first)
	$$(docker compose exec -T satellite-api storj-up credentials -e -s satellite-api:7777) && \
	docker run \
	--cap-add SYS_ADMIN --device /dev/fuse --security-opt apparmor:unconfined \
	--network integration-network-${BUILD_NUMBER} \
	-e GATEWAY_0_ADDR=gateway:9999 \
	-e "GATEWAY_0_ACCESS_KEY=$$AWS_ACCESS_KEY_ID" \
	-e "GATEWAY_0_SECRET_KEY=$$AWS_SECRET_ACCESS_KEY" \
	-v $$PWD:/build \
	-w /build \
	--name integration-gateway-st-tests-${BUILD_NUMBER}-$$TEST \
	--entrypoint /bin/bash \
	--rm storjlabs/ci:latest \
	-c "umask 0000; scripts/run-integration-tests.sh $$TEST"

.PHONY: integration-ceph-tests
integration-ceph-tests: ## (environment needs to be started first)
	$$(docker compose exec -T satellite-api storj-up credentials -e -s satellite-api:7777) && \
	docker run \
	--network integration-network-${BUILD_NUMBER} \
	-e GATEWAY_0_ADDR=gateway:9999 \
	-e "GATEWAY_0_ACCESS_KEY=$$AWS_ACCESS_KEY_ID" \
	-e "GATEWAY_0_SECRET_KEY=$$AWS_SECRET_ACCESS_KEY" \
	-v $$PWD:/build \
	-w /build \
	--name integration-ceph-tests-${BUILD_NUMBER}-$$TEST \
	--entrypoint /bin/bash \
	--user "$$(id -u):$$(id -g)" \
	--rm storjlabs/ci:latest \
	-c "testsuite/ceph-s3-tests/run.sh"

.PHONY: integration-mint-tests
integration-mint-tests: ## Run mint test suite (environment needs to be started first)
	$$(docker compose exec -T satellite-api storj-up credentials -e -s satellite-api:7777) && \
	docker run \
	--network integration-network-${BUILD_NUMBER} \
	-e SERVER_ENDPOINT=gateway:9999 \
	-e "ACCESS_KEY=$$AWS_ACCESS_KEY_ID" \
	-e "SECRET_KEY=$$AWS_SECRET_ACCESS_KEY" \
	-e ENABLE_HTTPS=0 \
	--name integration-mint-tests-${BUILD_NUMBER}-$$TEST \
	--rm storjlabs/gateway-mint:latest $$TEST

.PHONY: integration-splunk-tests
integration-splunk-tests: ## Run splunk test suite (environment needs to be started first)
	$$(docker compose exec -T satellite-api storj-up credentials -e -s satellite-api:7777) && \
	docker run \
	--network integration-network-${BUILD_NUMBER} \
	-e ENDPOINT=gateway:9999 \
	-e "AWS_ACCESS_KEY_ID=$$AWS_ACCESS_KEY_ID" \
	-e "AWS_SECRET_ACCESS_KEY=$$AWS_SECRET_ACCESS_KEY" \
	-e SECURE=0 \
	--name integration-splunk-tests-${BUILD_NUMBER} \
	--rm storjlabs/splunk-s3-tests:latest

.PHONY: integration-image-build
integration-image-build:
	CGO_ENABLED=0 ./scripts/build-image.sh ${BUILD_NUMBER} ${GO_VERSION}

	git clone --filter blob:none --no-checkout https://github.com/storj/storj
	storj-up init minimal,db && \
		storj-up build remote github minimal -c $$(git -C storj rev-list --exclude='*rc*' --tags --max-count=1) -s && \
		docker compose -p storj-up-integration build

.PHONY: integration-network-create
integration-network-create:
	docker network create integration-network-${BUILD_NUMBER}

.PHONY: integration-network-remove
integration-network-remove:
	-docker network remove integration-network-${BUILD_NUMBER}

.PHONY: integration-services-start
integration-services-start:
	storj-up network set minimal,db integration-network-${BUILD_NUMBER} && \
	storj-up network unset minimal,db default && \
	storj-up env setenv satellite-api STORJ_CONSOLE_SIGNUP_ACTIVATION_CODE_ENABLED=false && \
	storj-up env setenv satellite-api STORJ_METAINFO_USE_BUCKET_LEVEL_OBJECT_VERSIONING=true && \
	storj-up env setenv satellite-api STORJ_METAINFO_OBJECT_LOCK_ENABLED=true && \
	storj-up env setenv satellite-api STORJ_METAINFO_DELETE_OBJECTS_ENABLED=true && \
	docker compose up -d && \
	storj-up health

	$$(docker compose exec -T satellite-api storj-up credentials -p -e -s satellite-api:7777) && \
	docker run \
	--network integration-network-${BUILD_NUMBER} --network-alias gateway \
	--name integration-gateway-${BUILD_NUMBER} \
	--rm -d storjlabs/gateway:${BUILD_NUMBER} run \
		--server.address 0.0.0.0:9999 \
		--access "$$STORJ_ACCESS" \
		--minio.access-key "$$AWS_ACCESS_KEY_ID" \
		--minio.secret-key "$$AWS_SECRET_ACCESS_KEY" \
		--s3.fully-compatible-listing
