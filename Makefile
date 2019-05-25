BASH ?= bash
GO ?= go
RM ?= rm

GENERATE_SOURCES = $(wildcard cmd/fisy/*.go)

fisy_SOURCES = $(wildcard cmd/fisy/*.go)

.PHONY: all
all: cmd/fisy/version.go
	[ -e bin ] || mkdir -p bin
	$(GO) generate $(GENERATE_SOURCES)
	$(GO) build -o bin/fisy $(fisy_SOURCES)

.PHONY: clean
clean:
	$(RM) -r bin

.PHONY: check
check:
	$(GO) test github.com/tommie/fisy/fs
