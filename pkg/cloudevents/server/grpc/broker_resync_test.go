package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	certificatesv1 "k8s.io/api/certificates/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"google.golang.org/grpc"
	addonv1alpha1 "open-cluster-management.io/api/addon/v1alpha1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workv1 "open-cluster-management.io/api/work/v1"

	addoncodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/addon/v1alpha1"
	clustercodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/cluster"
	csrcodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/csr"
	leasecodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/lease"
	workpayload "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/payload"
	workcodec "open-cluster-management.io/sdk-go/pkg/cloudevents/clients/work/source/codec"
	grpcoptions "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	grpcv2 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/v2/grpc"
	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server"
)

// testResyncService is a simple in-memory service for testing resync functionality
type testResyncService struct {
	resources map[string]*cloudevents.Event // resourceID -> event
	handler   server.EventHandler
	mu        sync.RWMutex
}

func newTestResyncService() *testResyncService {
	return &testResyncService{
		resources: make(map[string]*cloudevents.Event),
	}
}

func (s *testResyncService) List(ctx context.Context, opts cetypes.ListOptions) ([]*cloudevents.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	evts := make([]*cloudevents.Event, 0, len(s.resources))
	for _, evt := range s.resources {
		// Filter by cluster name if needed
		if opts.ClusterName != "" {
			clusterName, _ := evt.Extensions()[cetypes.ExtensionClusterName].(string)
			if clusterName != opts.ClusterName {
				continue
			}
		}
		evts = append(evts, evt)
	}
	return evts, nil
}

func (s *testResyncService) HandleStatusUpdate(ctx context.Context, evt *cloudevents.Event) error {
	// Not needed for resync tests
	return nil
}

func (s *testResyncService) RegisterHandler(ctx context.Context, handler server.EventHandler) {
	s.handler = handler
}

// Helper to add/update/delete resources
func (s *testResyncService) setResource(evt *cloudevents.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resourceID := evt.Extensions()[cetypes.ExtensionResourceID].(string)
	s.resources[resourceID] = evt
}

func (s *testResyncService) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resources = make(map[string]*cloudevents.Event)
}

// Helper functions to create events for different resource types

func createClusterEvent(clusterName, resourceID string, version int64, deletionTimestamp *metav1.Time) (*cloudevents.Event, error) {
	cluster := &clusterv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              clusterName,
			UID:               types.UID(resourceID),
			ResourceVersion:   fmt.Sprintf("%d", version),
			DeletionTimestamp: deletionTimestamp,
		},
		Spec: clusterv1.ManagedClusterSpec{
			HubAcceptsClient: true,
		},
	}

	codec := clustercodec.NewManagedClusterCodec()
	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: clustercodec.ManagedClusterEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}
	return codec.Encode("test-source", eventType, cluster)
}

func createAddonEvent(clusterName, resourceID string, version int64, deletionTimestamp *metav1.Time) (*cloudevents.Event, error) {
	addon := &addonv1alpha1.ManagedClusterAddOn{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-addon",
			Namespace:         clusterName,
			UID:               types.UID(resourceID),
			ResourceVersion:   fmt.Sprintf("%d", version),
			DeletionTimestamp: deletionTimestamp,
		},
		Spec: addonv1alpha1.ManagedClusterAddOnSpec{
			InstallNamespace: "test-ns",
		},
	}

	codec := addoncodec.NewManagedClusterAddOnCodec()
	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: addoncodec.ManagedClusterAddOnEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}
	return codec.Encode("test-source", eventType, addon)
}

func createLeaseEvent(clusterName, resourceID string, version int64, deletionTimestamp *metav1.Time) (*cloudevents.Event, error) {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-lease",
			Namespace:         clusterName,
			UID:               types.UID(resourceID),
			ResourceVersion:   fmt.Sprintf("%d", version),
			DeletionTimestamp: deletionTimestamp,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: func() *string { s := "holder"; return &s }(),
		},
	}

	codec := leasecodec.NewLeaseCodec()
	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: leasecodec.LeaseEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}
	return codec.Encode("test-source", eventType, lease)
}

func createWorkEvent(clusterName, resourceID string, generation int64, deletionTimestamp *metav1.Time) (*cloudevents.Event, error) {
	work := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-work",
			Namespace:         clusterName,
			UID:               types.UID(resourceID),
			Generation:        generation,
			DeletionTimestamp: deletionTimestamp,
		},
		Spec: workv1.ManifestWorkSpec{
			Workload: workv1.ManifestsTemplate{
				Manifests: []workv1.Manifest{},
			},
		},
	}

	codec := workcodec.NewManifestBundleCodec()
	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: workpayload.ManifestBundleEventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}
	return codec.Encode("test-source", eventType, work)
}

func createCSREvent(clusterName, resourceID string, version int64, deletionTimestamp *metav1.Time) (*cloudevents.Event, error) {
	csr := &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-csr",
			UID:               types.UID(resourceID),
			ResourceVersion:   fmt.Sprintf("%d", version),
			DeletionTimestamp: deletionTimestamp,
			Labels: map[string]string{
				clusterv1.ClusterNameLabelKey: clusterName,
			},
		},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request: []byte("test-csr-request"),
			Usages: []certificatesv1.KeyUsage{
				certificatesv1.UsageDigitalSignature,
			},
		},
	}

	codec := csrcodec.NewCSRCodec()
	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: csrcodec.CSREventDataType,
		SubResource:         cetypes.SubResourceSpec,
		Action:              cetypes.CreateRequestAction,
	}
	return codec.Encode("test-source", eventType, csr)
}

// Test resync for ManagedCluster
func TestBrokerResync_Cluster(t *testing.T) {
	testBrokerResync(t, "cluster", clustercodec.ManagedClusterEventDataType, createClusterEvent)
}

// Test resync for ManagedClusterAddOn
func TestBrokerResync_Addon(t *testing.T) {
	testBrokerResync(t, "addon", addoncodec.ManagedClusterAddOnEventDataType, createAddonEvent)
}

// Test resync for Lease
func TestBrokerResync_Lease(t *testing.T) {
	testBrokerResync(t, "lease", leasecodec.LeaseEventDataType, createLeaseEvent)
}

// Test resync for CSR
func TestBrokerResync_CSR(t *testing.T) {
	testBrokerResync(t, "csr", csrcodec.CSREventDataType, createCSREvent)
}

// Test resync for ManifestWork (uses generation instead of resourceVersion)
func TestBrokerResync_Work(t *testing.T) {
	clusterName := "test-cluster"

	// Setup gRPC broker
	grpcServer := grpc.NewServer()
	broker := NewGRPCBroker(NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, broker)

	svc := newTestResyncService()
	broker.RegisterService(context.Background(), workpayload.ManifestBundleEventDataType, svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()
	defer grpcServer.GracefulStop()

	// Create agent client
	grpcOptions := grpcoptions.NewGRPCOptions()
	grpcOptions.Dialer = &grpcoptions.GRPCDialer{URL: lis.Addr().String()}
	agentOptions := grpcv2.NewAgentOptions(grpcOptions, clusterName, "test-agent", workpayload.ManifestBundleEventDataType)

	if err := agentOptions.CloudEventsTransport.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer agentOptions.CloudEventsTransport.Close(ctx)

	if err := agentOptions.CloudEventsTransport.Subscribe(ctx); err != nil {
		t.Fatal(err)
	}

	// Track received events
	receivedEvents := make(map[string]*cloudevents.Event)
	var mu sync.Mutex
	receivedCh := make(chan struct{}, 10)

	// Start receiving events
	go func() {
		if err := agentOptions.CloudEventsTransport.Receive(ctx, func(ctx context.Context, event cloudevents.Event) {
			mu.Lock()
			resourceID := event.Extensions()[cetypes.ExtensionResourceID].(string)
			receivedEvents[resourceID] = &event
			mu.Unlock()
			select {
			case receivedCh <- struct{}{}:
			default:
			}
		}); err != nil && err != context.Canceled {
			t.Errorf("receive error: %v", err)
		}
	}()

	t.Run("work: empty resync gets all resources", func(t *testing.T) {
		// Setup: Create 3 resources on source
		work1, _ := createWorkEvent(clusterName, "work-1", 100, nil)
		work2, _ := createWorkEvent(clusterName, "work-2", 200, nil)
		work3, _ := createWorkEvent(clusterName, "work-3", 300, nil)
		svc.setResource(work1)
		svc.setResource(work2)
		svc.setResource(work3)

		// Trigger resync with empty versions
		payload := map[string]any{
			"resourceVersions": []map[string]any{},
		}
		evt := cetypes.NewEventBuilder("test-agent", cetypes.CloudEventsType{
			CloudEventsDataType: workpayload.ManifestBundleEventDataType,
			SubResource:         cetypes.SubResourceSpec,
			Action:              cetypes.ResyncRequestAction,
		}).WithClusterName(clusterName).NewEvent()
		if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
			t.Fatal(err)
		}

		if result := agentOptions.CloudEventsTransport.Send(ctx, evt); result != nil {
			t.Fatalf("failed to send resync request: %v", result)
		}

		// Wait for events to be received
		waitForEvents(t, receivedCh, 3, 5*time.Second)

		// Verify all resources received
		mu.Lock()
		count := len(receivedEvents)
		mu.Unlock()

		if count != 3 {
			t.Errorf("expected 3 resources, got %d", count)
		}

		// Cleanup
		mu.Lock()
		receivedEvents = make(map[string]*cloudevents.Event)
		mu.Unlock()
		svc.clear()
	})

	t.Run("work: agent has same generation, no update", func(t *testing.T) {
		work1, _ := createWorkEvent(clusterName, "work-same", 100, nil)
		svc.setResource(work1)

		// Agent claims to have same generation (using generic resourceVersions format)
		payload := map[string]any{
			"resourceVersions": []map[string]any{
				{"resourceID": "work-same", "resourceVersion": 100},
			},
		}
		evt := cetypes.NewEventBuilder("test-agent", cetypes.CloudEventsType{
			CloudEventsDataType: workpayload.ManifestBundleEventDataType,
			SubResource:         cetypes.SubResourceSpec,
			Action:              cetypes.ResyncRequestAction,
		}).WithClusterName(clusterName).NewEvent()
		if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
			t.Fatal(err)
		}

		if result := agentOptions.CloudEventsTransport.Send(ctx, evt); result != nil {
			t.Fatalf("failed to send resync request: %v", result)
		}

		// Should NOT receive any events (same version)
		select {
		case <-receivedCh:
			t.Error("expected no events, but received one")
		case <-time.After(2 * time.Second):
			// Good, no events received
		}

		svc.clear()
	})

	t.Run("work: agent has older generation, gets update", func(t *testing.T) {
		work1, _ := createWorkEvent(clusterName, "work-update", 200, nil)
		svc.setResource(work1)

		// Agent claims to have older generation
		payload := map[string]any{
			"resourceVersions": []map[string]any{
				{"resourceID": "work-update", "resourceVersion": 100},
			},
		}
		evt := cetypes.NewEventBuilder("test-agent", cetypes.CloudEventsType{
			CloudEventsDataType: workpayload.ManifestBundleEventDataType,
			SubResource:         cetypes.SubResourceSpec,
			Action:              cetypes.ResyncRequestAction,
		}).WithClusterName(clusterName).NewEvent()
		if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
			t.Fatal(err)
		}

		if result := agentOptions.CloudEventsTransport.Send(ctx, evt); result != nil {
			t.Fatalf("failed to send resync request: %v", result)
		}

		// Should receive the updated resource
		waitForEvents(t, receivedCh, 1, 5*time.Second)

		mu.Lock()
		_, exists := receivedEvents["work-update"]
		mu.Unlock()

		if !exists {
			t.Error("expected to receive updated work")
		}

		mu.Lock()
		receivedEvents = make(map[string]*cloudevents.Event)
		mu.Unlock()
		svc.clear()
	})
}

// Generic test function for resync that works for cluster, addon, lease, csr
func testBrokerResync(
	t *testing.T,
	resourceName string,
	dataType cetypes.CloudEventsDataType,
	createEventFn func(string, string, int64, *metav1.Time) (*cloudevents.Event, error),
) {
	clusterName := "test-cluster"

	// Setup gRPC broker
	grpcServer := grpc.NewServer()
	broker := NewGRPCBroker(NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, broker)

	svc := newTestResyncService()
	broker.RegisterService(context.Background(), dataType, svc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()
	defer grpcServer.GracefulStop()

	// Create agent client
	grpcOptions := grpcoptions.NewGRPCOptions()
	grpcOptions.Dialer = &grpcoptions.GRPCDialer{URL: lis.Addr().String()}
	agentOptions := grpcv2.NewAgentOptions(grpcOptions, clusterName, "test-agent", dataType)

	if err := agentOptions.CloudEventsTransport.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer agentOptions.CloudEventsTransport.Close(ctx)

	if err := agentOptions.CloudEventsTransport.Subscribe(ctx); err != nil {
		t.Fatal(err)
	}

	// Track received events
	receivedEvents := make(map[string]*cloudevents.Event)
	var mu sync.Mutex
	receivedCh := make(chan struct{}, 10)

	go func() {
		if err := agentOptions.CloudEventsTransport.Receive(ctx, func(ctx context.Context, event cloudevents.Event) {
			mu.Lock()
			resourceID := event.Extensions()[cetypes.ExtensionResourceID].(string)
			receivedEvents[resourceID] = &event
			mu.Unlock()
			select {
			case receivedCh <- struct{}{}:
			default:
			}
		}); err != nil && err != context.Canceled {
			t.Errorf("receive error: %v", err)
		}
	}()

	t.Run(fmt.Sprintf("%s: empty resync gets all resources", resourceName), func(t *testing.T) {
		// Clean state
		svc.clear()
		drainChannel(receivedCh)
		mu.Lock()
		receivedEvents = make(map[string]*cloudevents.Event)
		mu.Unlock()

		// Setup: Create 3 resources on source
		res1, _ := createEventFn(clusterName, "res-1", 100, nil)
		res2, _ := createEventFn(clusterName, "res-2", 200, nil)
		res3, _ := createEventFn(clusterName, "res-3", 300, nil)
		svc.setResource(res1)
		svc.setResource(res2)
		svc.setResource(res3)

		// Trigger resync with empty versions
		payload := map[string]any{
			"resourceVersions": []map[string]any{},
		}
		evt := cetypes.NewEventBuilder("test-agent", cetypes.CloudEventsType{
			CloudEventsDataType: dataType,
			SubResource:         cetypes.SubResourceSpec,
			Action:              cetypes.ResyncRequestAction,
		}).WithClusterName(clusterName).NewEvent()
		if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
			t.Fatal(err)
		}

		if result := agentOptions.CloudEventsTransport.Send(ctx, evt); result != nil {
			t.Fatalf("failed to send resync request: %v", result)
		}

		// Wait for events
		waitForEvents(t, receivedCh, 3, 5*time.Second)

		mu.Lock()
		count := len(receivedEvents)
		mu.Unlock()

		if count != 3 {
			t.Errorf("expected 3 resources, got %d", count)
		}
	})

	t.Run(fmt.Sprintf("%s: agent has same version, no update", resourceName), func(t *testing.T) {
		// Clean state
		svc.clear()
		drainChannel(receivedCh)
		mu.Lock()
		receivedEvents = make(map[string]*cloudevents.Event)
		mu.Unlock()

		res1, _ := createEventFn(clusterName, "res-same", 100, nil)
		svc.setResource(res1)

		// Agent claims to have same version
		payload := map[string]any{
			"resourceVersions": []map[string]any{
				{"resourceID": "res-same", "resourceVersion": 100},
			},
		}
		evt := cetypes.NewEventBuilder("test-agent", cetypes.CloudEventsType{
			CloudEventsDataType: dataType,
			SubResource:         cetypes.SubResourceSpec,
			Action:              cetypes.ResyncRequestAction,
		}).WithClusterName(clusterName).NewEvent()
		if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
			t.Fatal(err)
		}

		if result := agentOptions.CloudEventsTransport.Send(ctx, evt); result != nil {
			t.Fatalf("failed to send resync request: %v", result)
		}

		// Should NOT receive any events
		select {
		case <-receivedCh:
			t.Error("expected no events for same version, but received one")
		case <-time.After(2 * time.Second):
			// Good, no events
		}
	})

	t.Run(fmt.Sprintf("%s: agent has older version, gets update", resourceName), func(t *testing.T) {
		// Clean state
		svc.clear()
		drainChannel(receivedCh)
		mu.Lock()
		receivedEvents = make(map[string]*cloudevents.Event)
		mu.Unlock()

		res1, _ := createEventFn(clusterName, "res-update", 200, nil)
		svc.setResource(res1)

		// Agent claims to have older version
		payload := map[string]any{
			"resourceVersions": []map[string]any{
				{"resourceID": "res-update", "resourceVersion": 100},
			},
		}
		evt := cetypes.NewEventBuilder("test-agent", cetypes.CloudEventsType{
			CloudEventsDataType: dataType,
			SubResource:         cetypes.SubResourceSpec,
			Action:              cetypes.ResyncRequestAction,
		}).WithClusterName(clusterName).NewEvent()
		if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
			t.Fatal(err)
		}

		if result := agentOptions.CloudEventsTransport.Send(ctx, evt); result != nil {
			t.Fatalf("failed to send resync request: %v", result)
		}

		// Should receive update
		waitForEvents(t, receivedCh, 1, 5*time.Second)

		mu.Lock()
		_, exists := receivedEvents["res-update"]
		mu.Unlock()

		if !exists {
			t.Error("expected to receive updated resource")
		}
	})

	t.Run(fmt.Sprintf("%s: resource deleted on source, agent gets deletion", resourceName), func(t *testing.T) {
		svc.clear()
		drainChannel(receivedCh)
		mu.Lock()
		receivedEvents = make(map[string]*cloudevents.Event)
		mu.Unlock()

		// Give time for cleanup to settle
		time.Sleep(100 * time.Millisecond)

		// Source has no resources, but agent claims to have one
		payload := map[string]any{
			"resourceVersions": []map[string]any{
				{"resourceID": "res-deleted", "resourceVersion": 100},
			},
		}
		evt := cetypes.NewEventBuilder("test-agent", cetypes.CloudEventsType{
			CloudEventsDataType: dataType,
			SubResource:         cetypes.SubResourceSpec,
			Action:              cetypes.ResyncRequestAction,
		}).WithClusterName(clusterName).NewEvent()
		if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
			t.Fatal(err)
		}

		if result := agentOptions.CloudEventsTransport.Send(ctx, evt); result != nil {
			t.Fatalf("failed to send resync request: %v", result)
		}

		// Should receive deletion event
		waitForEvents(t, receivedCh, 1, 5*time.Second)

		mu.Lock()
		event, exists := receivedEvents["res-deleted"]
		allEvents := make(map[string]bool)
		for k := range receivedEvents {
			allEvents[k] = true
		}
		mu.Unlock()

		if !exists {
			t.Errorf("expected to receive deletion event for res-deleted, but got events for: %v", allEvents)
		}

		// Verify it has deletion timestamp
		if event != nil {
			if _, ok := event.Extensions()[cetypes.ExtensionDeletionTimestamp]; !ok {
				t.Error("expected deletion timestamp in event")
			}
		}
	})

	t.Run(fmt.Sprintf("%s: resource with deletion timestamp always sent", resourceName), func(t *testing.T) {
		// Clean state
		svc.clear()
		drainChannel(receivedCh)
		mu.Lock()
		receivedEvents = make(map[string]*cloudevents.Event)
		mu.Unlock()

		now := metav1.Now()
		res1, _ := createEventFn(clusterName, "res-deleting", 50, &now)
		svc.setResource(res1)

		// Agent claims to have newer version (100 > 50), but should still receive it
		payload := map[string]any{
			"resourceVersions": []map[string]any{
				{"resourceID": "res-deleting", "resourceVersion": 100},
			},
		}
		evt := cetypes.NewEventBuilder("test-agent", cetypes.CloudEventsType{
			CloudEventsDataType: dataType,
			SubResource:         cetypes.SubResourceSpec,
			Action:              cetypes.ResyncRequestAction,
		}).WithClusterName(clusterName).NewEvent()
		if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
			t.Fatal(err)
		}

		if result := agentOptions.CloudEventsTransport.Send(ctx, evt); result != nil {
			t.Fatalf("failed to send resync request: %v", result)
		}

		// Should receive the deleting resource
		waitForEvents(t, receivedCh, 1, 5*time.Second)

		mu.Lock()
		event, exists := receivedEvents["res-deleting"]
		mu.Unlock()

		if !exists {
			t.Error("expected to receive deleting resource")
		}

		if event != nil {
			if _, ok := event.Extensions()[cetypes.ExtensionDeletionTimestamp]; !ok {
				t.Error("expected deletion timestamp in deleting resource")
			}
		}
	})

	t.Run(fmt.Sprintf("%s: mixed scenario", resourceName), func(t *testing.T) {
		// Clean state
		svc.clear()
		drainChannel(receivedCh)
		mu.Lock()
		receivedEvents = make(map[string]*cloudevents.Event)
		mu.Unlock()

		// Setup:
		// - res-1: version 200 on source, agent has 100 (should receive update)
		// - res-2: version 50 on both (should NOT receive)
		// - res-3: new on source (should receive)
		// - res-4: deleted on source, agent has it (should receive deletion)
		res1, _ := createEventFn(clusterName, "res-1", 200, nil)
		res2, _ := createEventFn(clusterName, "res-2", 50, nil)
		res3, _ := createEventFn(clusterName, "res-3", 10, nil)
		svc.setResource(res1)
		svc.setResource(res2)
		svc.setResource(res3)

		payload := map[string]any{
			"resourceVersions": []map[string]any{
				{"resourceID": "res-1", "resourceVersion": 100},
				{"resourceID": "res-2", "resourceVersion": 50},
				{"resourceID": "res-4", "resourceVersion": 20},
			},
		}
		evt := cetypes.NewEventBuilder("test-agent", cetypes.CloudEventsType{
			CloudEventsDataType: dataType,
			SubResource:         cetypes.SubResourceSpec,
			Action:              cetypes.ResyncRequestAction,
		}).WithClusterName(clusterName).NewEvent()
		if err := evt.SetData(cloudevents.ApplicationJSON, payload); err != nil {
			t.Fatal(err)
		}

		if result := agentOptions.CloudEventsTransport.Send(ctx, evt); result != nil {
			t.Fatalf("failed to send resync request: %v", result)
		}

		// Should receive 3 events: res-1 (update), res-3 (new), res-4 (deletion)
		waitForEvents(t, receivedCh, 3, 5*time.Second)

		mu.Lock()
		count := len(receivedEvents)
		_, hasRes1 := receivedEvents["res-1"]
		_, hasRes2 := receivedEvents["res-2"]
		_, hasRes3 := receivedEvents["res-3"]
		_, hasRes4 := receivedEvents["res-4"]
		mu.Unlock()

		if count != 3 {
			t.Errorf("expected 3 events, got %d", count)
		}
		if !hasRes1 {
			t.Error("expected res-1 (updated)")
		}
		if hasRes2 {
			t.Error("did not expect res-2 (same version)")
		}
		if !hasRes3 {
			t.Error("expected res-3 (new)")
		}
		if !hasRes4 {
			t.Error("expected res-4 (deleted)")
		}
	})
}

// Helper to drain channel
func drainChannel(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// Helper to wait for expected number of events
func waitForEvents(t *testing.T, ch chan struct{}, expected int, timeout time.Duration) {
	t.Helper()
	received := 0
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for received < expected {
		select {
		case <-ch:
			received++
		case <-timer.C:
			t.Fatalf("timeout waiting for events: expected %d, got %d", expected, received)
			return
		}
	}
}
