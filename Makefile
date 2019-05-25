BASH ?= bash
GO ?= go
RM ?= rm

GENERATE_SOURCES = $(wildcard cmd/fisy/*.go)

fisy_SOURCES = $(wildcard cmd/fisy/*.go)

.PHONY: all
all:
	[ -e bin ] || mkdir -p bin
	$(GO) generate $(GENERATE_SOURCES)
	$(GO) build -o bin/fisy $(fisy_SOURCES)

.PHONY: configure
configure:
	$(GO) get ./...

.PHONY: clean
clean:
	$(RM) -r bin

.PHONY: check
check:
	$(GO) test ./...
