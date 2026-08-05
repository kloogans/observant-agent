# observant-agent build targets.
#
# CGO is always off. The agent must be a static binary.

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
PKG     := ./cmd/observant-agent
OUT     := dist
LDFLAGS := -s -w -X main.version=$(VERSION)
GOBUILD := CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)"

.PHONY: all
all: check build

.PHONY: build
build:
	@./build.sh $(VERSION)

.PHONY: dev
dev:
	$(GOBUILD) -o $(OUT)/observant-agent $(PKG)

.PHONY: check
check: fmt vet test

.PHONY: fmt
fmt:
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test ./...

.PHONY: race
race:
	go test -race ./...

.PHONY: once
once:
	go run $(PKG) -once

.PHONY: selfcheck
selfcheck:
	go run $(PKG) -selfcheck

.PHONY: clean
clean:
	rm -rf $(OUT)
