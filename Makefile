GO_VERSION ?= 1.17.12
BRANCH_NAME ?= $(shell git rev-parse --abbrev-ref HEAD | sed "s!/!-!g")
LATEST_DEV_TAG := dev

#
# Common
#

.DEFAULT_GOAL := help
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
	go install github.com/storj/ci/check-deferloop@latest
	go install github.com/storj/ci/check-downgrades@latest

	# staticcheck:
	go install honnef.co/go/tools/cmd/staticcheck@latest

	# golangci-lint:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.45.2

	# shellcheck (TODO(artur): Windows)
ifneq ($(shell which apt-get),)
	sudo apt-get install -y shellcheck
else ifneq ($(shell which brew),)
	brew install shellcheck
else
	$(error Can't install shellcheck without a supported package manager)
endif

.PHONY: install-hooks
install-hooks: ## Install helpful Git hooks
	ln -s .git/hooks/pre-commit githooks/pre-commit

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
	check-deferloop ./...
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
	check-deferloop ./...
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

# todo(artur, sean): these extra ldflags are required for the minio object
# browser to function, but should be automated. Use storj.io/minio/buildscripts/gen-ldflags.go
LDFLAGS := -X storj.io/minio/cmd.Version=2022-04-19T11:13:21Z \
	-X storj.io/minio/cmd.ReleaseTag=DEVELOPMENT.2022-04-19T11-13-21Z \
	-X storj.io/minio/cmd.CommitID=ae15cc41053bb0a65e543f71d81646dd2318fe10 \
	-X storj.io/minio/cmd.ShortCommitID=ae15cc41053b

ifeq (${BRANCH_NAME},main)
	TAG := $(shell git rev-parse --short HEAD)-go${GO_VERSION}
	BRANCH_NAME :=
else
	TAG := $(shell git rev-parse --short HEAD)-${BRANCH_NAME}-go${GO_VERSION}
	ifneq (,$(shell git describe --tags --exact-match --match "v[0-9]*\.[0-9]*\.[0-9]*"))
		LATEST_STABLE_TAG := latest
	endif
endif

DOCKER_BUILD := docker build --build-arg TAG=${TAG}

##@ Dependencies

.PHONY: build-dev-deps
build-dev-deps: ## Install dependencies for builds
	go get golang.org/x/tools/cover
	go get github.com/josephspurrier/goversioninfo/cmd/goversioninfo

.PHONY: goimports-fix
goimports-fix: ## Applies goimports to every go file (excluding vendored files)
	goimports -w -local storj.io $$(find . -type f -name '*.go' -not -path "*/vendor/*")

.PHONY: goimports-st
goimports-st: ## Applies goimports to every go file in `git status` (ignores untracked files)
	@git status --porcelain -uno|grep .go|grep -v "^D"|sed -E 's,\w+\s+(.+->\s+)?,,g'|xargs -I {} goimports -w -local storj.io {}

.PHONY: build-packages
build-packages: build-packages-normal build-packages-race ## Test docker images locally
build-packages-normal:
	go build -v ./...
build-packages-race:
	go build -v -race ./...

##@ Build

.PHONY: images
images: gateway-image ## Build gateway Docker images
	echo Built version: ${TAG}

.PHONY: gateway-image
gateway-image: ## Build gateway Docker image
	${DOCKER_BUILD} --pull=true -t storjlabs/gateway:${TAG}-amd64 \
		-f Dockerfile .
	${DOCKER_BUILD} --pull=true -t storjlabs/gateway:${TAG}-arm32v6 \
		--build-arg=GOARCH=arm --build-arg=DOCKER_ARCH=arm32v6 \
		-f Dockerfile .
	${DOCKER_BUILD} --pull=true -t storjlabs/gateway:${TAG}-arm64v8 \
		--build-arg=GOARCH=arm64 --build-arg=DOCKER_ARCH=arm64v8 \
		-f Dockerfile .
	docker tag storjlabs/gateway:${TAG}-amd64 storjlabs/gateway:${LATEST_DEV_TAG}

binaries: ## Build gateway binaries (jenkins)
	CGO_ENABLED=0 LDFLAGS="${LDFLAGS}" storj-release \
		--build-name gateway \
		--build-tags kqueue \
		--go-version "${GO_VERSION}" \
		--branch "${BRANCH_NAME}" \
		--skip-osarches "freebsd/amd64"

	# freebsd/amd64 requires CGO_ENABLED=1: https://github.com/storj/gateway-st/issues/62
	CGO_ENABLED=1 LDFLAGS="${LDFLAGS}" storj-release \
		--build-name gateway \
		--build-tags kqueue \
		--go-version "${GO_VERSION}" \
		--branch "${BRANCH_NAME}" \
		--osarches "freebsd/amd64"

##@ Deploy

.PHONY: push-images
push-images: ## Push Docker images to Docker Hub (jenkins)
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
			&& docker manifest annotate storjlabs/$$c:$$t storjlabs/$$c:${TAG}-arm64v8 --os linux --arch arm64 \
			&& docker manifest push --purge storjlabs/$$c:$$t \
		; done \
	; done

.PHONY: binaries-upload
binaries-upload: ## Upload binaries to Google Storage (jenkins)
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
	cd "release/${TAG}"; gsutil -m cp -r *.zip "gs://storj-v3-alpha-builds/${TAG}/"

##@ Clean

.PHONY: clean
clean: test-docker-clean binaries-clean clean-images ## Clean docker test environment, local release binaries, and local Docker images

.PHONY: binaries-clean
binaries-clean: ## Remove all local release binaries (jenkins)
	rm -rf release

.PHONY: clean-images
clean-images:
	-docker rmi storjlabs/gateway:${TAG}

.PHONY: test-docker-clean
test-docker-clean: ## Clean up Docker environment used in test-docker target
	-docker-compose down --rmi all

.PHONY: bump-dependencies
bump-dependencies:
	go get storj.io/common@main storj.io/private@main storj.io/uplink@main
	go mod tidy
	cd testsuite;\
		go get storj.io/common@main storj.io/storj@main storj.io/uplink@main;\
		go mod tidy
