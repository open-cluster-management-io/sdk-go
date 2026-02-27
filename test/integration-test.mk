ENSURE_ENVTEST_SCRIPT := ./ci/envtest/ensure-envtest.sh
ENSURE_ENVTEST_SCRIPT_URL := https://raw.githubusercontent.com/open-cluster-management-io/sdk-go/main/ci/envtest/ensure-envtest.sh

# Resolve envtest: use local script if available, otherwise download from remote
ENVTEST_SCRIPT_CMD = $(if $(wildcard $(ENSURE_ENVTEST_SCRIPT)),$(ENSURE_ENVTEST_SCRIPT),curl -fsSL $(ENSURE_ENVTEST_SCRIPT_URL) | bash)

# Reusable target — add as a prerequisite to any target that needs envtest binaries.
# Uses $(eval) so the script only runs when this target is actually triggered,
# not on every make invocation (e.g. make verify).
.PHONY: envtest-setup
envtest-setup:
	$(eval export KUBEBUILDER_ASSETS := $(shell $(ENVTEST_SCRIPT_CMD)))
	@if [ -z "$(KUBEBUILDER_ASSETS)" ]; then \
		echo "Error: ensure-envtest.sh failed to produce KUBEBUILDER_ASSETS path"; \
		exit 1; \
	fi
	@echo "KUBEBUILDER_ASSETS=$(KUBEBUILDER_ASSETS)"

clean-integration-test:
	$(RM) ./integration.test
	$(RM) ./cloudevents.test
	$(RM) ./basecontroller.test
	$(RM) ./servingcertcontroller.test
.PHONY: clean-integration-test

clean: clean-integration-test

integration: envtest-setup test-cloudevents-integration test-basecontroller-integration test-servingcertcontroller-integration
.PHONY: integration

test-cloudevents-integration:
	go test -c ./test/integration/cloudevents
	./cloudevents.test -ginkgo.slowSpecThreshold=15 -ginkgo.v -ginkgo.failFast -v=5
.PHONY: test-cloudevents-integration

test-basecontroller-integration:
	go test -c ./test/integration/basecontroller -o ./basecontroller.test
	./basecontroller.test -ginkgo.slowSpecThreshold=15 -ginkgo.v -ginkgo.failFast
.PHONY: test-basecontroller-integration

test-servingcertcontroller-integration:
	go test -c ./test/integration/servingcertcontroller -o ./servingcertcontroller.test
	./servingcertcontroller.test -ginkgo.slowSpecThreshold=15 -ginkgo.v -ginkgo.failFast
.PHONY: test-servingcertcontroller-integration
