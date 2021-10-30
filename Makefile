BASH ?= bash
GO ?= go
RM ?= rm

GOBUILDFLAGS ?=
GOTESTFLAGS ?=

build_source = $(shell git remote show -n "$$(git remote show -n | head -n1)" | sed -E -e 's;\s*Fetch URL: (.*);\1; p ; d' || :)
build_branch = $(shell git symbolic-ref --short --quiet HEAD || :)
build_revision = $(shell git rev-parse --short HEAD || :)
build_isclean = $(shell git status -u --porcelain=v1 | grep -q . && echo false || echo true)
build_date = $(shell date --utc +'%Y-%m-%d')

GO_LDFLAGS = \
        -X github.com/tommie/fisy/internal/build.buildSource="$(build_source)" \
        -X github.com/tommie/fisy/internal/build.buildBranch="$(build_branch)" \
        -X github.com/tommie/fisy/internal/build.buildRevision="$(build_revision)" \
        -X github.com/tommie/fisy/internal/build.buildIsClean="$(build_isclean)" \
        -X github.com/tommie/fisy/internal/build.buildDate="$(build_date)"
EXTRA_GOBUILDFLAGS = -ldflags "$(GO_LDFLAGS)"

GENERATE_SOURCES = $(wildcard cmd/fisy/*.go)

fisy_SOURCES = $(wildcard cmd/fisy/*.go)

.PHONY: all
all: go-generate
	[ -e bin ] || mkdir -p bin
	$(GO) build $(EXTRA_GOBUILDFLAGS) $(GOBUILDFLAGS) -o bin/fisy $(fisy_SOURCES)

.PHONY: configure
configure: go-generate
	$(GO) get $(EXTRA_GOBUILDFLAGS) $(GOBUILDFLAGS) ./...

.PHONY: clean
clean:
	$(RM) -r bin

.PHONY: check
check:
	$(GO) test $(EXTRA_GOBUILDFLAGS) $(GOBUILDFLAGS) $(GOTESTFLAGS) ./...

.PHONY: go-generate
go-generate:
	$(GO) generate $(EXTRA_GOBUILDFLAGS) $(GOBUILDFLAGS) $(GENERATE_SOURCES)
