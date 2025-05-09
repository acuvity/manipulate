MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash -o pipefail

export GO111MODULE = on

default: lint test

lint:
	# --enable=unparam
	golangci-lint run \
		--timeout=5m \
		--disable-all \
		--exclude-use-default=false \
		--exclude=package-comments \
		--exclude=unused-parameter \
		--exclude=dot-imports \
		--enable=errcheck \
		--enable=goimports \
		--enable=ineffassign \
		--enable=revive \
		--enable=unused \
		--enable=staticcheck \
		--enable=unconvert \
		--enable=misspell \
		--enable=prealloc \
		--enable=nakedret \
		--enable=typecheck \
		./...

test:
	go test ./... -vet off -race -cover -covermode=atomic -coverprofile=unit_coverage.out

sec:
	gosec -quiet ./...
