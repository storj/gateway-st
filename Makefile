.PHONY: help
help:
	@awk 'BEGIN { \
		FS = ":.*##"; \
		printf "\nUsage:\n  make \033[36m<target>\033[0m\n" \
	} \
	/^[a-zA-Z0-9_-]+:.*?##/ { \
		printf "  \033[36m%-32s\033[0m %s\n", $$1, $$2 \
	} \
	/^##@/ { \
		printf "\n\033[1m%s\033[0m\n", substr($$0, 5) \
	}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

GO_VERSION  ?= 1.26.4
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

# Namespaces integration resources to avoid CI-agent collisions.
BUILD_NUMBER ?= ${TAG}

##@ Verification

.PHONY: verify
verify: lint cross-vet test ## Run the full verification suite (umbrella)
	# Not a dep so it observes the final worktree under -j.
	$(MAKE) verify-clean

.PHONY: verify-clean
verify-clean: ## Fail if verification left untracked files in the worktree
	check-clean-directory

##@ Verification/Dependencies

# Track https://github.com/storj/ci/blob/main/images/tools.
GOLANGCI_LINT_VERSION ?= v2.12.2

.PHONY: install-dev-dependencies
install-dev-dependencies: ## Install tools required by `make verify` (assumes Go and cURL are present)
	go install github.com/storj/ci/check-mod-tidy@latest
	go install github.com/storj/ci/check-copyright@latest
	go install github.com/storj/ci/check-large-files@latest
	go install github.com/storj/ci/check-imports@latest
	go install github.com/storj/ci/check-peer-constraints@latest
	go install github.com/storj/ci/check-atomic-align@latest
	go install github.com/storj/ci/check-monkit@latest
	go install github.com/storj/ci/check-errs@latest
	go install github.com/storj/ci/check-downgrades@latest
	go install github.com/storj/ci/check-clean-directory@latest

	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install github.com/jstemmer/go-junit-report/v2@v2.1.0

ifneq ($(shell which apt-get),)
	sudo apt-get install -y shellcheck
else ifneq ($(shell which brew),)
	brew install shellcheck
else
	$(error Can't install shellcheck without a supported package manager)
endif

##@ Verification/Lint

GOLANGCI_LINT_CONFIG ?= ../ci/.golangci.yml
GOLANGCI_LINT_CONFIG_TESTSUITE ?= ../../ci/.golangci.yml

.PHONY: lint
lint: lint-go lint-shell lint-testsuite ## Run all lint checks

.PHONY: lint-go
lint-go: ## Run Go linters on the root module
	check-mod-tidy
	check-copyright
	check-large-files
	check-imports -race ./...
	check-peer-constraints -race
	check-atomic-align ./...
	check-monkit ./...
	check-errs ./...
	staticcheck ./...
	golangci-lint run --config ${GOLANGCI_LINT_CONFIG}
	check-downgrades

.PHONY: lint-shell
lint-shell: ## Run shellcheck on all shell scripts
	# `+` (not `\;`) so a failing shellcheck fails the recipe.
	find . \( -path ./.build -o -path ./testsuite@tmp \) -prune -o -name "*.sh" -type f -exec "shellcheck" "-x" "--format=gcc" {} +

.PHONY: lint-testsuite
lint-testsuite: ## Run Go linters on the testsuite module
	$(MAKE) -C testsuite -f ../Makefile lint-testsuite-do

.PHONY: lint-testsuite-do
lint-testsuite-do:
	check-imports -race ./...
	check-atomic-align ./...
	check-monkit ./...
	check-errs ./...
	staticcheck ./...
	golangci-lint run --config ${GOLANGCI_LINT_CONFIG_TESTSUITE}

##@ Verification/Cross-Vet

.PHONY: cross-vet
cross-vet: ## Run `go vet` across the supported GOOS/GOARCH matrix
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

	# TODO(artur): enable once upstream supports them:
	# GOOS=windows GOARCH=arm   go vet ./...
	# GOOS=windows GOARCH=arm64 go vet ./...

	# kqueue tag avoids Cgo on darwin.
	GOOS=darwin  GOARCH=amd64 go vet -tags kqueue ./...
	GOOS=darwin  GOARCH=arm64 go vet -tags kqueue ./...

##@ Verification/Test

JSON  ?= false
SHORT ?= true

.PHONY: test
test: test-main test-testsuite ## Run the unit-test suite for both modules

.PHONY: test-main
test-main: ## Run the unit-test suite for the root module
	go test -json=$(JSON) -p 16 -parallel 4 -race -short=$(SHORT) -vet=off ./...

.PHONY: test-testsuite
test-testsuite: ## Run the unit-test suite for the testsuite module
	$(MAKE) -C testsuite -f ../Makefile test-testsuite-do

.PHONY: test-testsuite-do
test-testsuite-do:
	go vet ./...
	go test -json=$(JSON) -p 16 -parallel 4 -race -short=$(SHORT) -vet=off ./...

##@ Verification/Integration

# docker-compose project names disallow dots.
INTEGRATION_PROJECT     ?= integration-$(subst .,-,$(BUILD_NUMBER))
INTEGRATION_NETWORK     ?= integration-network-$(BUILD_NUMBER)
INTEGRATION_GATEWAY     ?= integration-gateway-$(BUILD_NUMBER)
INTEGRATION_SATELLITE   ?= satellite-api:7777
INTEGRATION_COMPOSE     := docker compose -p $(INTEGRATION_PROJECT)
INTEGRATION_CREDENTIALS := $(INTEGRATION_COMPOSE) exec -T satellite-api storj-up credentials -e -s $(INTEGRATION_SATELLITE)

# Storj satellite revision storj-up builds. Defaults to testsuite/go.mod's pin.
STORJ_REF ?= $(shell awk '$$1 == "storj.io/storj" {print $$2}' testsuite/go.mod)

.PHONY: integration-run
integration-run: ## Bring up the env, run all integration tests, tear it down (purges even on failure)
	$(MAKE) integration-env-start
	$(MAKE) integration-tests; rc=$$?; $(MAKE) integration-env-purge; exit $$rc

.PHONY: integration-tests
integration-tests: integration-gateway-st-tests integration-gateway-st-tests-s3fs integration-mint-tests integration-ceph-tests ## Run all integration tests (environment needs to be started first)

.PHONY: integration-env-start
integration-env-start: integration-image-build integration-network-create integration-services-start ## Start the integration environment

.PHONY: integration-env-purge
integration-env-purge: ## Tear down everything created by integration-env-start (idempotent)
	-docker ps -qf network=$(INTEGRATION_NETWORK) | xargs -r docker stop --timeout=1
	-docker rm -f $(INTEGRATION_GATEWAY)
	# No --rmi: keep shared base images (redis, spanner-emulator, storjup/build).
	-$(INTEGRATION_COMPOSE) down --volumes --remove-orphans --timeout 1 2>/dev/null
	-docker image ls -qf label=build=$(BUILD_NUMBER) | xargs -r docker rmi -f
	-docker rmi -f storj
	-docker network remove $(INTEGRATION_NETWORK)
	-rm -rf storj edge.Dockerfile storj.Dockerfile docker-compose.yaml

.PHONY: integration-env-logs
integration-env-logs: ## Retrieve logs from integration services
	-$(INTEGRATION_COMPOSE) logs
	-docker logs $(INTEGRATION_GATEWAY)

.PHONY: integration-image-build
integration-image-build:
	CGO_ENABLED=0 ./scripts/build-image.sh ${BUILD_NUMBER} ${GO_VERSION}

	storj-up init minimal,db && \
		storj-up build remote github minimal -b $(STORJ_REF) -c $(STORJ_REF) -s && \
		$(INTEGRATION_COMPOSE) build

.PHONY: integration-network-create
integration-network-create:
	docker network create $(INTEGRATION_NETWORK)

.PHONY: integration-services-start
integration-services-start:
	storj-up network set minimal,db $(INTEGRATION_NETWORK) && \
	storj-up network unset minimal,db default && \
	storj-up env setenv satellite-api STORJ_METAINFO_DELETE_OBJECTS_ENABLED=true && \
	storj-up env setenv satellite-api STORJ_METAINFO_BUCKET_TAGGING_ENABLED=true && \
	storj-up port remove postgres 5432 && \
	storj-up port add postgres 5432 -e 5433 && \
	$(INTEGRATION_COMPOSE) up -d && \
	storj-up health -p 5433

	$$($(INTEGRATION_CREDENTIALS) -p) && \
	docker run \
	--network $(INTEGRATION_NETWORK) --network-alias gateway \
	--name $(INTEGRATION_GATEWAY) \
	--rm -d storjlabs/gateway:${BUILD_NUMBER} run \
		--server.address 0.0.0.0:9999 \
		--access "$$STORJ_ACCESS" \
		--minio.access-key "$$AWS_ACCESS_KEY_ID" \
		--minio.secret-key "$$AWS_SECRET_ACCESS_KEY" \
		--s3.fully-compatible-listing \
		--s3.upload-part-copy.enable

.PHONY: integration-gateway-st-tests
integration-gateway-st-tests: ## Run a single gateway-st subtest as $$TEST (environment needs to be started first)
	$$($(INTEGRATION_CREDENTIALS)) && \
	docker run \
	--network $(INTEGRATION_NETWORK) \
	-u "$$(id -u):$$(id -g)" \
	-e GATEWAY_0_ADDR=gateway:9999 \
	-e "GATEWAY_0_ACCESS_KEY=$$AWS_ACCESS_KEY_ID" \
	-e "GATEWAY_0_SECRET_KEY=$$AWS_SECRET_ACCESS_KEY" \
	-e HOME=/tmp -e GOPATH=/tmp/go \
	-v $$PWD:/build \
	-w /build \
	--name integration-gateway-st-tests-${BUILD_NUMBER}-$$TEST \
	--rm storjlabs/ci:latest \
	testsuite/integration/$$TEST.sh

# s3fs needs FUSE so it can't run as the host user.
.PHONY: integration-gateway-st-tests-s3fs
integration-gateway-st-tests-s3fs: ## Run the gateway-st s3fs subtest (privileged; environment needs to be started first)
	$$($(INTEGRATION_CREDENTIALS)) && \
	docker run \
	--cap-add SYS_ADMIN --device /dev/fuse --security-opt apparmor:unconfined \
	--network $(INTEGRATION_NETWORK) \
	-e GATEWAY_0_ADDR=gateway:9999 \
	-e "GATEWAY_0_ACCESS_KEY=$$AWS_ACCESS_KEY_ID" \
	-e "GATEWAY_0_SECRET_KEY=$$AWS_SECRET_ACCESS_KEY" \
	-v $$PWD:/build \
	-w /build \
	--name integration-gateway-st-tests-s3fs-${BUILD_NUMBER} \
	--rm storjlabs/ci:latest \
	testsuite/integration/s3fs.sh

# umask 0000 because the container runs as root and writes to bind-mounted /build/.build/.
.PHONY: integration-ceph-tests
integration-ceph-tests: ## Run ceph s3-tests suite (environment needs to be started first)
	$$($(INTEGRATION_CREDENTIALS)) && \
	docker run \
	--network $(INTEGRATION_NETWORK) \
	-e GATEWAY_0_ADDR=gateway:9999 \
	-e "GATEWAY_0_ACCESS_KEY=$$AWS_ACCESS_KEY_ID" \
	-e "GATEWAY_0_SECRET_KEY=$$AWS_SECRET_ACCESS_KEY" \
	-v $$PWD:/build \
	-w /build \
	--name integration-ceph-tests-${BUILD_NUMBER}-$$TEST \
	--entrypoint /bin/bash \
	--rm python:3.13-bookworm \
	-c "umask 0000; testsuite/ceph-s3-tests/run.sh"

.PHONY: integration-mint-tests
integration-mint-tests: ## Run mint test suite (environment needs to be started first)
	$$($(INTEGRATION_CREDENTIALS)) && \
	docker run \
	--network $(INTEGRATION_NETWORK) \
	-e SERVER_ENDPOINT=gateway:9999 \
	-e "ACCESS_KEY=$$AWS_ACCESS_KEY_ID" \
	-e "SECRET_KEY=$$AWS_SECRET_ACCESS_KEY" \
	-e ENABLE_HTTPS=0 \
	--name integration-mint-tests-${BUILD_NUMBER}-$$TEST \
	--rm storjlabs/gateway-mint:latest $$TEST

##@ Local development/Helpers

.PHONY: bump-code-dependencies
bump-code-dependencies: ## Bump storj/* deps to the latest tip of main
	go get storj.io/minio@main   && go mod tidy && cd testsuite && go mod tidy && cd .. && \
	go get storj.io/common@main  && go mod tidy && cd testsuite && go mod tidy && cd .. && \
	go get storj.io/uplink@main  && go mod tidy && cd testsuite && go mod tidy && \
	go get storj.io/storj@latest && go mod tidy

.PHONY: install-hooks
install-hooks: ## Install helpful Git hooks
	ln -s ../../githooks/pre-commit .git/hooks/pre-commit

##@ Release/Private Jenkins/Build

DOCKER_BUILD   := docker build --build-arg TAG=${TAG}
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
binaries: ${BINARIES} ## Build gateway binaries
	# TODO(artur): we could use a bit of caching here, but that's not strictly necessary for now
	docker run --rm \
		-v $$PWD:/usr/src/gateway \
		-w /usr/src/gateway \
		-e GOCACHE=/tmp/go-pkg \
		-u $$(id -u):$$(id -g) \
		golang:"${GO_VERSION}" scripts/build_linux.sh "release/${TAG}" "${LDFLAGS}"

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
