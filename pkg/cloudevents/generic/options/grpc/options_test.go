package grpc

import (
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"k8s.io/utils/ptr"
	clienttesting "open-cluster-management.io/sdk-go/pkg/testing"
)

func TestBuildGRPCOptionsFromFlags(t *testing.T) {
	cases := []struct {
		name             string
		config           string
		expectedOptions  *GRPCOptions
		expectedErrorMsg string
	}{
		{
			name:             "empty config",
			config:           "",
			expectedErrorMsg: "url is required",
		},
		{
			name:             "tls config without clientCertFile",
			config:           "{\"url\":\"test\",\"clientCertData\":\"dGVzdAo=\"}",
			expectedErrorMsg: "either both or none of clientCertFile and clientKeyFile must be set",
		},
		{
			name:             "token config without caFile",
			config:           "{\"url\":\"test\",\"token\":\"test\"}",
			expectedErrorMsg: "setting token requires authority certificates",
		},
		{
			name:   "customized options",
			config: "{\"url\":\"test\"}",
			expectedOptions: &GRPCOptions{
				Dialer: &GRPCDialer{
					URL: "test",
					KeepAliveOptions: KeepAliveOptions{
						Enable:              false,
						Time:                30 * time.Second,
						Timeout:             10 * time.Second,
						PermitWithoutStream: false,
					},
				},
			},
		},
		{
			name:   "customized options with yaml format",
			config: "url: test",
			expectedOptions: &GRPCOptions{
				Dialer: &GRPCDialer{
					URL: "test",
					KeepAliveOptions: KeepAliveOptions{
						Enable:              false,
						Time:                30 * time.Second,
						Timeout:             10 * time.Second,
						PermitWithoutStream: false,
					},
				},
			},
		},
		{
			name:   "customized options with keepalive",
			config: "{\"url\":\"test\",\"keepAliveConfig\":{\"enable\":true,\"time\":10s,\"timeout\":5s,\"permitWithoutStream\":true}}",
			expectedOptions: &GRPCOptions{
				Dialer: &GRPCDialer{
					URL: "test",
					KeepAliveOptions: KeepAliveOptions{
						Enable:              true,
						Time:                10 * time.Second,
						Timeout:             5 * time.Second,
						PermitWithoutStream: true,
					},
				},
			},
		},
		{
			name:   "customized options with ServerHealthinessTimeout",
			config: "{\"url\":\"test\",\"serverHealthinessTimeout\":\"10s\"}",
			expectedOptions: &GRPCOptions{
				Dialer: &GRPCDialer{
					URL: "test",
					KeepAliveOptions: KeepAliveOptions{
						Enable:              false,
						Time:                30 * time.Second,
						Timeout:             10 * time.Second,
						PermitWithoutStream: false,
					},
				},
				ServerHealthinessTimeout: ptr.To(10 * time.Second),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			file, err := clienttesting.WriteToTempFile("grpc-config-test-", []byte(c.config))
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(file.Name())

			options, err := BuildGRPCOptionsFromFlags(file.Name())
			if err != nil {
				if err.Error() != c.expectedErrorMsg {
					t.Errorf("unexpected err %v", err)
				}
			}

			if !cmp.Equal(options, c.expectedOptions, cmpopts.IgnoreUnexported(GRPCDialer{})) {
				t.Errorf("unexpected options %+v", options)
			}
		})
	}
}

func TestDialExtraDialOpts(t *testing.T) {
	cases := []struct {
		name          string
		extraDialOpts []grpc.DialOption
	}{
		{
			name: "with extra dial opts",
			extraDialOpts: []grpc.DialOption{
				grpc.WithConnectParams(grpc.ConnectParams{
					Backoff: backoff.Config{
						BaseDelay:  1 * time.Second,
						Multiplier: 1.6,
						Jitter:     0.2,
						MaxDelay:   5 * time.Second,
					},
					MinConnectTimeout: 5 * time.Second,
				}),
			},
		},
		{
			name: "without extra dial opts",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dialer := &GRPCDialer{
				URL:           "localhost:0",
				ExtraDialOpts: c.extraDialOpts,
			}

			conn, err := dialer.Dial()
			if err != nil {
				t.Fatalf("Dial() failed: %v", err)
			}
			defer conn.Close()

			if conn == nil {
				t.Fatal("Dial() returned nil connection")
			}
		})
	}
}
