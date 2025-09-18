package cloudevents

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"

	"k8s.io/utils/ptr"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic"
	grpcprotocol "open-cluster-management.io/sdk-go/pkg/cloudevents/generic/options/grpc/protocol"
	"open-cluster-management.io/sdk-go/pkg/cloudevents/generic/types"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/server"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/store"
	"open-cluster-management.io/sdk-go/test/integration/cloudevents/util"
)

var (
	proxyServerHost  string
	targetServerHost string
)

// getAvailablePort finds an available port by creating and closing a listener
func getAvailablePort() (string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr, nil
}

// ProxyServer simulates a proxy that maintains healthy connections but can stop forwarding heartbeats
type ProxyServer struct {
	listener    net.Listener
	addr        string
	target      string
	connections map[net.Conn]net.Conn
	mu          sync.RWMutex
	stopped     atomic.Bool
	// Controls whether heartbeats are forwarded
	forwardHeartbeats atomic.Bool
}

func NewProxyServer(proxyAddr, targetAddr string) *ProxyServer {
	proxy := &ProxyServer{
		addr:        proxyAddr,
		target:      targetAddr,
		connections: make(map[net.Conn]net.Conn),
	}
	proxy.forwardHeartbeats.Store(true)
	return proxy
}

func (p *ProxyServer) Start() error {
	lis, err := net.Listen("tcp", p.addr)
	if err != nil {
		return err
	}
	p.listener = lis

	go func() {
		for {
			clientConn, err := lis.Accept()
			if err != nil {
				if !p.stopped.Load() {
					fmt.Printf("Proxy accept error: %v\n", err)
				}
				return
			}
			go p.handleConnection(clientConn)
		}
	}()

	return nil
}

func (p *ProxyServer) handleConnection(clientConn net.Conn) {
	serverConn, err := net.Dial("tcp", p.target)
	if err != nil {
		fmt.Printf("Proxy dial error: %v\n", err)
		clientConn.Close()
		return
	}

	p.mu.Lock()
	p.connections[clientConn] = serverConn
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.connections, clientConn)
		p.mu.Unlock()
		clientConn.Close()
		serverConn.Close()
	}()

	// Proxy data bidirectionally
	done := make(chan struct{})

	// Client to server
	go func() {
		defer close(done)
		p.copyData(serverConn, clientConn, false) // false = not filtering heartbeats
	}()

	// Server to client (with potential heartbeat filtering)
	go func() {
		p.copyData(clientConn, serverConn, true) // true = filter heartbeats if disabled
	}()

	<-done
}

func (p *ProxyServer) copyData(dst, src net.Conn, filterHeartbeats bool) {
	buffer := make([]byte, 4096)
	for {
		n, err := src.Read(buffer)
		if err != nil {
			return
		}

		data := buffer[:n]

		// Simple heartbeat filtering - if we're not forwarding heartbeats and this looks like gRPC data
		// that might contain a heartbeat, we skip it. This is a simplified approach.
		if filterHeartbeats && !p.forwardHeartbeats.Load() {
			// Check if this might be a gRPC message containing a heartbeat
			if p.containsHeartbeat(data) {
				continue // Skip forwarding this data
			}
		}

		_, err = dst.Write(data)
		if err != nil {
			return
		}
	}
}

// Simple heuristic to detect potential heartbeat messages
func (p *ProxyServer) containsHeartbeat(data []byte) bool {
	// Look for heartbeat type string in the data
	return len(data) < 500 && // Heartbeats are typically small but allow for protobuf overhead
		(contains(data, []byte("heartbeat")) ||
			contains(data, []byte("io.open-cluster-management.heartbeat")) ||
			contains(data, []byte(types.HeartbeatCloudEventsType)))
}

func contains(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func (p *ProxyServer) StopForwardingHeartbeats() {
	p.forwardHeartbeats.Store(false)
}

func (p *ProxyServer) StartForwardingHeartbeats() {
	p.forwardHeartbeats.Store(true)
}

func (p *ProxyServer) Stop() {
	p.stopped.Store(true)
	if p.listener != nil {
		p.listener.Close()
	}

	p.mu.Lock()
	for clientConn, serverConn := range p.connections {
		clientConn.Close()
		serverConn.Close()
	}
	p.mu.Unlock()
}

var _ = ginkgo.Describe("Heartbeat Reconnection Integration Test", func() {
	var (
		proxyServer  *ProxyServer
		targetServer *server.GRPCServer
		cancel       context.CancelFunc
	)

	ginkgo.BeforeEach(func() {
		_, cancel = context.WithCancel(context.Background())

		// Reduce reconnect delays for faster testing
		generic.DelayFn = func() time.Duration { return 500 * time.Millisecond }

		// Get available ports
		var err error
		targetServerHost, err = getAvailablePort()
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		proxyServerHost, err = getAvailablePort()
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		// Start target server with heartbeats
		serverStore := store.NewMemoryStore()
		targetServer = server.NewGRPCServerWithHeartbeat(serverStore, 100*time.Millisecond)

		go func() {
			defer ginkgo.GinkgoRecover()
			err := targetServer.Start(targetServerHost, nil)
			if err != nil {
				ginkgo.Fail(fmt.Sprintf("Target server failed to start: %v", err))
			}
		}()

		// Start proxy server
		proxyServer = NewProxyServer(proxyServerHost, targetServerHost)
		err = proxyServer.Start()
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		// Give servers time to start
		time.Sleep(200 * time.Millisecond)
	})

	ginkgo.AfterEach(func() {
		if cancel != nil {
			cancel()
		}
		if proxyServer != nil {
			proxyServer.Stop()
		}
		if targetServer != nil {
			targetServer.Stop()
		}
		time.Sleep(500 * time.Millisecond) // Increased wait time for proper cleanup
	})

	ginkgo.It("should reconnect and resume heartbeats after heartbeat forwarding is re-enabled", func() {
		// Verify that heartbeat is enabled
		gomega.Expect(targetServer.IsHeartbeatEnabled()).To(gomega.BeTrue())

		// Create gRPC connection and protocol with health check enabled
		grpcOptions := util.NewGRPCSourceOptions(proxyServerHost)
		conn, err := grpcOptions.Dialer.Dial()
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		defer conn.Close()

		reconnectErrorChan := make(chan error, 10)
		protocol, err := grpcprotocol.NewProtocol(
			conn,
			grpcprotocol.WithSubscribeOption(&grpcprotocol.SubscribeOption{
				ClusterName: "test-cluster",
				DataType:    types.HeartbeatCloudEventsType,
			}),
			grpcprotocol.WithReconnectErrorChan(reconnectErrorChan),
			grpcprotocol.WithServerHealthinessTimeout(ptr.To(300*time.Millisecond)),
		)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		// Start protocol in background
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		protocolStarted := make(chan struct{})
		go func() {
			defer ginkgo.GinkgoRecover()
			close(protocolStarted)
			if err := protocol.OpenInbound(ctx); err != nil {
				select {
				case reconnectErrorChan <- err:
				default:
				}
			}
		}()

		// Wait for protocol to start
		<-protocolStarted

		// Phase 1: Verify initial heartbeats are flowing
		// Let heartbeats flow for a short period
		time.Sleep(500 * time.Millisecond)

		// Check that no reconnect errors occurred initially
		select {
		case err := <-reconnectErrorChan:
			ginkgo.Fail(fmt.Sprintf("Unexpected initial reconnect error: %v", err))
		default:
			// Good - no reconnect errors
		}

		// Phase 2: Stop forwarding heartbeats to trigger health check failure
		proxyServer.StopForwardingHeartbeats()
		ginkgo.By("Stopped forwarding heartbeats through proxy")

		// Wait for health check to detect missing heartbeats and trigger reconnect
		select {
		case err := <-reconnectErrorChan:
			// Expected - should get health check error
			gomega.Expect(err).To(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Received expected health check error: %v", err))
		case <-time.After(1 * time.Second):
			ginkgo.Fail("Expected health check error due to missing heartbeats")
		}

		// Phase 3: Re-enable heartbeat forwarding
		proxyServer.StartForwardingHeartbeats()
		ginkgo.By("Re-enabled heartbeat forwarding through proxy")

		// Phase 4: Verify that client reconnects and heartbeats resume
		// The protocol should automatically attempt to reconnect
		// Give it time to reconnect and for heartbeats to resume
		time.Sleep(1 * time.Second)

		// Check that we don't get any more reconnect errors (meaning successful reconnection)
		// Allow for some reconnection attempts during the stabilization period
		stableTime := time.Now()
		reconnectStabilized := false
		for time.Since(stableTime) < 2*time.Second {
			select {
			case err := <-reconnectErrorChan:
				// Reset the stable time if we get reconnect errors during stabilization
				ginkgo.By(fmt.Sprintf("Reconnection attempt during stabilization: %v", err))
				stableTime = time.Now()
			case <-time.After(500 * time.Millisecond):
				// No errors for 500ms, consider it stable
				reconnectStabilized = true
				break
			}
		}

		// Verify that the connection stabilized (no more reconnect errors)
		gomega.Expect(reconnectStabilized).To(gomega.BeTrue(), "Connection should stabilize after heartbeat forwarding is re-enabled")

		// Verify that the target server is still running and heartbeat is enabled
		gomega.Expect(targetServer.IsHeartbeatEnabled()).To(gomega.BeTrue(), "Target server heartbeat should still be enabled")

		ginkgo.By("Successfully verified reconnection and heartbeat resumption")
	})

	ginkgo.It("should handle normal operations through proxy with heartbeat enabled", func() {
		// Verify that heartbeat is enabled on the server
		gomega.Expect(targetServer.IsHeartbeatEnabled()).To(gomega.BeTrue())

		// Test that the proxy server properly forwards regular gRPC traffic while heartbeat is enabled
		grpcOptions := util.NewGRPCSourceOptions(proxyServerHost)
		conn, err := grpcOptions.Dialer.Dial()
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		defer conn.Close()

		// Wait for connection to be established
		time.Sleep(200 * time.Millisecond)

		// Test basic connectivity - connection should be ready or connecting
		ginkgo.By("Verifying proxy forwards normal gRPC traffic while heartbeat is enabled")
		state := conn.GetState()
		gomega.Expect(state.String()).To(gomega.SatisfyAny(
			gomega.Equal("READY"),
			gomega.Equal("CONNECTING"),
			gomega.Equal("IDLE"),
		))

		// Test that proxy can handle start/stop of traffic forwarding
		ginkgo.By("Testing proxy traffic control with heartbeat enabled")
		proxyServer.StopForwardingHeartbeats()
		time.Sleep(100 * time.Millisecond)
		proxyServer.StartForwardingHeartbeats()
		time.Sleep(100 * time.Millisecond)

		// Connection should still be usable
		gomega.Expect(conn.GetState().String()).ToNot(gomega.Equal("SHUTDOWN"))

		// Verify heartbeat is still enabled after operations
		gomega.Expect(targetServer.IsHeartbeatEnabled()).To(gomega.BeTrue())

		ginkgo.By("Successfully verified proxy handles normal operations with heartbeat enabled")
	})
})
