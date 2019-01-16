LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.ReleaseVersion=$(shell git describe --tags --dirty="-dev")"
LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-enterprise-tools/pkg/utils.GoVersion=$(shell go version)"


CURDIR   := $(shell pwd)
GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build
GOTEST   := CGO_ENABLED=1 $(GO) test
PACKAGES := $$(go list ./... | grep -vE 'vendor|cmd|tests|tests2')
FILES    := $$(find . -name "*.go" | grep -vE "vendor")
TOPDIRS  := $$(ls -d */ | grep -vE "vendor")
SHELL    := /usr/bin/env bash
TEST_DIR := /tmp/dm_test

RACE_FLAG =
ifeq ("$(WITH_RACE)", "1")
	RACE_FLAG = -race
	GOBUILD   = CGO_ENABLED=1 $(GO) build
endif


ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"

.PHONY: build syncer loader test dm_integration_test_build integration_test \
	coverage check deps dm-worker dm-master dmctl

build: syncer loader check test dm-worker dm-master dmctl

dm-worker:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-worker ./cmd/dm-worker

dm-master:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-master ./cmd/dm-master

dmctl:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dmctl ./cmd/dm-ctl

syncer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/syncer ./cmd/syncer

loader:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/loader ./cmd/loader

test:
	bash -x ./wait_for_mysql.sh
	@export log_level=error; \
	$(GOTEST) -cover -race $(PACKAGES)

check: fmt lint vet

fmt:
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

errcheck:
	GO111MODULE=off go get github.com/kisielk/errcheck
	@echo "errcheck"
	@ errcheck -blank $(PACKAGES) | grep -v "_test\.go" | awk '{print} END{if(NR>0) {exit 1}}'

lint:
	@ GO111MODULE=off go build -o bin/golint github.com/golang/lint/golint
	@echo "golint"
	@ ./bin/golint -set_exit_status $(PACKAGES)

vet:
	@echo "vet"
	@ $(GO) tool vet -all -shadow $(TOPDIRS) 2>&1 | tee /dev/stderr | awk '/shadows declaration/{next}{count+=1} END{if(count>0) {exit 1}}'

dm_integration_test_build:
	$(GOTEST) -c -race -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tidb-enterprise-tools/... \
		-o bin/dm-worker.test github.com/pingcap/tidb-enterprise-tools/cmd/dm-worker
	$(GOTEST) -c -race -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tidb-enterprise-tools/... \
		-o bin/dm-master.test github.com/pingcap/tidb-enterprise-tools/cmd/dm-master

integration_test:
	@which bin/tidb-server
	@which bin/sync_diff_inspector
	@which bin/mydumper
	@which bin/dm-master.test
	@which bin/dm-worker.test
	tests2/run.sh

coverage:
	GO111MODULE=off go get github.com/wadey/gocovmerge
	gocovmerge "$(TEST_DIR)"/cov.* > "$(TEST_DIR)/all_cov.out"
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
