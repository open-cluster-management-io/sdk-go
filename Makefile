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
# LDFLAGS are not needed for dummy builds (saving time on calling git commands)
GO_LD_FLAGS:=
# controller-gen setup
CONTROLLER_GEN_VERSION :=v0.11.3
CONTROLLER_GEN :=$(PERMANENT_TMP_GOPATH)/bin/controller-gen
ifneq "" "$(wildcard $(CONTROLLER_GEN))"
_controller_gen_installed_version = $(shell $(CONTROLLER_GEN) --version | awk '{print $$2}')
endif
controller_gen_dir :=$(abspath $(PERMANENT_TMP_GOPATH)/bin)

RUNTIME ?= podman
RUNTIME_IMAGE_NAME ?= openshift-api-generator

verify-gocilint:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.52.0
	go vet ./...
	${GOPATH}/bin/golangci-lint run --timeout=3m ./...

verify: verify-gocilint
