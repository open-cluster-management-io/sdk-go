SHELL :=/bin/bash

all: build
.PHONY: all

# Include the library makefile
include $(addprefix ./vendor/github.com/openshift/build-machinery-go/make/, \
	golang.mk \
	targets/openshift/deps.mk \
)

GO_PACKAGES :=$(addsuffix ...,$(addprefix ./,$(filter-out test/, $(filter-out vendor/,$(filter-out hack/,$(wildcard */))))))
GO_BUILD_PACKAGES :=$(GO_PACKAGES)
GO_BUILD_PACKAGES_EXPANDED :=$(GO_BUILD_PACKAGES)
GO_BUILD_FLAGS :=-trimpath
GO_TEST_FLAGS :=-race
# LDFLAGS are not needed for dummy builds (saving time on calling git commands)
GO_LD_FLAGS:=

verify-gocilint:
	@./ci/lint/run-lint.sh

verify-govet:
	go vet -mod=vendor ./...

verify: verify-gocilint

include ./test/integration-test.mk
