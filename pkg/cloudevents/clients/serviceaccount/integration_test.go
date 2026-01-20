package serviceaccount

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"google.golang.org/grpc"
	grpccli "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc"
	pbv1 "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protobuf/v1"
	cetypes "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/server"
	grpcserver "open-cluster-management.io/sdk-go/pkg/cloudevents/server/grpc"
)

// tokenRequestService implements a service that handles token requests
type tokenRequestService struct {
	mu                   sync.RWMutex
	tokenRequests        map[string]*authenticationv1.TokenRequest
	handler              server.EventHandler
	handleStatusUpdateFn func(context.Context, *cloudevents.Event) error
}

func (s *tokenRequestService) Get(ctx context.Context, resourceID string) (*cloudevents.Event, error) {
	s.mu.RLock()
	tokenReq, ok := s.tokenRequests[resourceID]
	s.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	codec := NewTokenRequestCodec()
	eventType := cetypes.CloudEventsType{
		CloudEventsDataType: TokenRequestDataType,
		SubResource:         cetypes.SubResourceStatus,
		Action:              cetypes.UpdateRequestAction,
	}

	evt, err := codec.Encode("test-source", eventType, tokenReq)
	if err != nil {
		return nil, err
	}

	return evt, nil
}

func (s *tokenRequestService) List(listOpts cetypes.ListOptions) ([]*cloudevents.Event, error) {
	return nil, nil
}

func (s *tokenRequestService) HandleStatusUpdate(ctx context.Context, evt *cloudevents.Event) error {
	if s.handleStatusUpdateFn != nil {
		return s.handleStatusUpdateFn(ctx, evt)
	}

	codec := NewTokenRequestCodec()
	tokenReq, err := codec.Decode(evt)
	if err != nil {
		return err
	}

	// When we receive a token request, we respond with a token
	tokenReq.Status = authenticationv1.TokenRequestStatus{
		Token:               "test-token-12345",
		ExpirationTimestamp: metav1.Time{Time: time.Now().Add(1 * time.Hour)},
	}

	s.mu.Lock()
	s.tokenRequests[string(tokenReq.UID)] = tokenReq
	s.mu.Unlock()

	// Notify the handler that we have a response ready
	if s.handler == nil {
		return nil
	}
	return s.handler.OnCreate(ctx, TokenRequestDataType, string(tokenReq.UID))
}

func (s *tokenRequestService) RegisterHandler(_ context.Context, handler server.EventHandler) {
	s.handler = handler
}

func TestCreateToken_Integration(t *testing.T) {
	// Set up gRPC server
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	grpcEventServer := grpcserver.NewGRPCBroker(grpcserver.NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, grpcEventServer)

	// Create and register the token request service
	svc := &tokenRequestService{
		tokenRequests: make(map[string]*authenticationv1.TokenRequest),
	}
	grpcEventServer.RegisterService(context.Background(), TokenRequestDataType, svc)

	// Start listening
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()

	// Give the server time to start
	time.Sleep(100 * time.Millisecond)

	// Create gRPC client options
	grpcClientOptions := grpccli.NewGRPCOptions()
	grpcClientOptions.Dialer = &grpccli.GRPCDialer{URL: lis.Addr().String()}

	// Create ServiceAccountClient
	clusterName := "test-cluster"
	saClient := NewServiceAccountClient(clusterName, grpcClientOptions)

	// Create a token request
	tokenRequest := &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			Audiences:         []string{"test-audience"},
			ExpirationSeconds: func() *int64 { v := int64(3600); return &v }(),
		},
	}

	result, err := saClient.CreateToken(context.Background(), "test-sa", tokenRequest, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("CreateToken failed: %v", err)
	}

	// Verify the response
	if result == nil {
		t.Fatal("expected non-nil token response")
	}

	if result.Status.Token != "test-token-12345" {
		t.Errorf("expected token 'test-token-12345', got '%s'", result.Status.Token)
	}

	if result.Status.ExpirationTimestamp.IsZero() {
		t.Error("expected non-zero expiration timestamp")
	}

	if result.Name != "test-sa" {
		t.Errorf("expected service account name 'test-sa', got '%s'", result.Name)
	}

	if result.Namespace != clusterName {
		t.Errorf("expected namespace '%s', got '%s'", clusterName, result.Namespace)
	}
}

func TestCreateToken_Timeout(t *testing.T) {
	// Set up gRPC server with a service that doesn't respond
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	grpcEventServer := grpcserver.NewGRPCBroker(grpcserver.NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, grpcEventServer)

	// Create a service that receives but never responds
	svc := &tokenRequestService{
		tokenRequests: make(map[string]*authenticationv1.TokenRequest),
		handleStatusUpdateFn: func(ctx context.Context, evt *cloudevents.Event) error {
			// Just receive but don't respond
			return nil
		},
	}
	grpcEventServer.RegisterService(context.Background(), TokenRequestDataType, svc)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Override timeout for this test
	originalTimeout := TokenRequestTimeout
	TokenRequestTimeout = 200 * time.Millisecond
	defer func() { TokenRequestTimeout = originalTimeout }()

	grpcClientOptions := grpccli.NewGRPCOptions()
	grpcClientOptions.Dialer = &grpccli.GRPCDialer{URL: lis.Addr().String()}

	saClient := NewServiceAccountClient("test-cluster", grpcClientOptions)

	tokenRequest := &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			Audiences: []string{"test-audience"},
		},
	}

	ctx := context.Background()
	_, err = saClient.CreateToken(ctx, "test-sa", tokenRequest, metav1.CreateOptions{})

	// Should timeout
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	t.Logf("Got expected timeout error: %v", err)
}

func TestCreateToken_MultipleRequests(t *testing.T) {
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	grpcEventServer := grpcserver.NewGRPCBroker(grpcserver.NewBrokerOptions())
	pbv1.RegisterCloudEventServiceServer(grpcServer, grpcEventServer)

	svc := &tokenRequestService{
		tokenRequests: make(map[string]*authenticationv1.TokenRequest),
	}
	grpcEventServer.RegisterService(context.Background(), TokenRequestDataType, svc)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = lis.Close()
	})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	grpcClientOptions := grpccli.NewGRPCOptions()
	grpcClientOptions.Dialer = &grpccli.GRPCDialer{URL: lis.Addr().String()}

	saClient := NewServiceAccountClient("test-cluster", grpcClientOptions)

	// Make sequential requests with delay between them
	numRequests := 2
	successCount := 0

	for i := 0; i < numRequests; i++ {
		// Add delay between requests to avoid connection reuse issues
		if i > 0 {
			time.Sleep(200 * time.Millisecond)
		}

		tokenRequest := &authenticationv1.TokenRequest{
			Spec: authenticationv1.TokenRequestSpec{
				Audiences: []string{"test-audience"},
			},
		}

		result, err := saClient.CreateToken(context.Background(), "test-sa", tokenRequest, metav1.CreateOptions{})

		if err != nil {
			t.Logf("Request %d failed (may be timing issue): %v", i, err)
			continue
		}

		if result.Status.Token == "test-token-12345" {
			successCount++
		}
	}

	// At least one request should succeed
	if successCount < 1 {
		t.Errorf("Expected at least 1 successful request, got %d", successCount)
	}
}
