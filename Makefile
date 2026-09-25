# Development tasks. `make` runs everything CI runs.
BUF ?= buf
GO ?= go

.PHONY: all build test lint generate examples golden validate check install

all: generate lint test examples

build:
	$(GO) build ./...

install:
	$(GO) install ./cmd/protoc-gen-temporal-asyncapi

test:
	$(GO) test ./...

lint:
	$(GO) vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "run gofmt" && exit 1)
	$(BUF) lint
	$(BUF) format --diff --exit-code

# Regenerates the Go bindings of the options.
generate:
	$(BUF) generate

# Regenerates the example AsyncAPI documents.
examples:
	$(BUF) generate --template examples/buf.gen.yaml

# Updates the golden files of the tests.
golden:
	$(GO) test ./internal/generator -update

# Validates the golden and example documents with the official AsyncAPI
# parser (needs Node.js).
validate:
	cd tools/asyncapi-validate && npm ci --silent && node validate.mjs

# Fails when generated files are out of date.
check: generate examples
	git diff --exit-code
