integration: test-cloudevents-integration
.PHONY: integration

test-cloudevents-integration:
	go test -tags=kafka -c ./test/integration/cloudevents
	./cloudevents.test -ginkgo.slowSpecThreshold=15 -ginkgo.v -ginkgo.failFast
.PHONY: test-cloudevents-integration
