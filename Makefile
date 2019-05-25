BASH ?= bash
GO ?= go
RM ?= rm

GENERATE_SOURCES = $(wildcard cmd/fisy/*.go)

fisy_SOURCES = $(wildcard cmd/fisy/*.go)

.PHONY: all
all: go-generate
	[ -e bin ] || mkdir -p bin
	$(GO) build -o bin/fisy $(fisy_SOURCES)

.PHONY: configure
configure: go-generate
	$(GO) get ./...

.PHONY: clean
clean:
	$(RM) -r bin

.PHONY: check
check:
	$(GO) test ./...

.PHONY: go-generate
go-generate:
	$(GO) generate $(GENERATE_SOURCES)
