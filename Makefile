MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash -o pipefail

export GO111MODULE = on

MONGO_TEST_MATRIX_LEGACY_IMAGE ?= mongo:4.4.26
MONGO_TEST_MATRIX_LATEST_IMAGE ?= mongo:latest
MONGO_TEST_MATRIX_LATEST_PATTERN ?= ^(TestOfficialManipulatorCRUDAndHelpersWithMemongo|TestMongoHelpersWithContextAcceptNilContext)$

default: lint test

install-tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	go install github.com/securego/gosec/cmd/gosec@master

lint:
	golangci-lint run \
		--timeout=5m \
		--disable=govet  \
		--enable=errcheck \
		--enable=ineffassign \
		--enable=unused \
		--enable=unconvert \
		--enable=misspell \
		--enable=prealloc \
		--enable=nakedret \
		--enable=unparam \
		--enable=nilerr \
		--enable=bodyclose \
		--enable=errorlint \
		./...

test:
	go test ./... -vet off -race -cover -covermode=atomic -coverprofile=unit_coverage.out

test-mongo-legacy:
	env -u DOCKER_HOST REQUIRE_MONGO=1 MONGO_TEST_DOCKER_IMAGE=$(MONGO_TEST_MATRIX_LEGACY_IMAGE) go test ./manipmongo/... -count=1

test-mongo-latest:
	env -u DOCKER_HOST REQUIRE_MONGO=1 MONGO_TEST_DOCKER_IMAGE=$(MONGO_TEST_MATRIX_LATEST_IMAGE) go test ./manipmongo -run '$(MONGO_TEST_MATRIX_LATEST_PATTERN)' -count=1
	env -u DOCKER_HOST REQUIRE_MONGO=1 MONGO_TEST_DOCKER_IMAGE=$(MONGO_TEST_MATRIX_LATEST_IMAGE) go test ./manipmongo/internal -count=1

test-mongo-matrix: test-mongo-legacy test-mongo-latest

sec:
	gosec -quiet ./...

remod:
	go get go.acuvity.ai/elemental@master
	go get go.acuvity.ai/regolithe@master
	go get go.acuvity.ai/wsc@master
	go mod tidy
