BASH ?= bash
GO ?= go
RM ?= rm

GOBUILDFLAGS ?=
GOTESTFLAGS ?=

GENERATE_SOURCES = $(wildcard cmd/fisy/*.go)

fisy_SOURCES = $(wildcard cmd/fisy/*.go)

.PHONY: all
all: go-generate
	[ -e bin ] || mkdir -p bin
	$(GO) build $(GOBUILDFLAGS) -o bin/fisy $(fisy_SOURCES)

.PHONY: configure
configure: go-generate
	$(GO) get $(GOBUILDFLAGS) ./...

.PHONY: clean
clean:
	$(RM) -r bin

.PHONY: check
check:
	$(GO) test $(GOBUILDFLAGS) $(GOTESTFLAGS) ./...

.PHONY: go-generate
go-generate:
	$(GO) generate $(GOBUILDFLAGS) $(GENERATE_SOURCES)
