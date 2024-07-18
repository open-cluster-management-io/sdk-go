package grpc

import (
	"log"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestBuildGRPCOptionsFromFlags(t *testing.T) {
	file, err := os.CreateTemp("", "grpc-config-test-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())

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
			config:           "{\"url\":\"test\",\"clientCertFile\":\"test\"}",
			expectedErrorMsg: "either both or none of clientCertFile and clientKeyFile must be set",
		},
		{
			name:             "tls config without caFile",
			config:           "{\"url\":\"test\",\"clientCertFile\":\"test\",\"clientKeyFile\":\"test\"}",
			expectedErrorMsg: "setting clientCertFile and clientKeyFile requires caFile",
		},
		{
			name:   "customized options",
			config: "{\"url\":\"test\"}",
			expectedOptions: &GRPCOptions{
				URL: "test",
				KeepAliveOptions: KeepAliveOptions{
					Enable:              false,
					Time:                30 * time.Second,
					Timeout:             10 * time.Second,
					PermitWithoutStream: false,
				},
			},
		},
		{
			name:   "customized options with yaml format",
			config: "url: test",
			expectedOptions: &GRPCOptions{
				URL: "test",
				KeepAliveOptions: KeepAliveOptions{
					Enable:              false,
					Time:                30 * time.Second,
					Timeout:             10 * time.Second,
					PermitWithoutStream: false,
				},
			},
		},
		{
			name:   "customized options with keepalive",
			config: "{\"url\":\"test\",\"keepAliveConfig\":{\"enable\":true,\"time\":10s,\"timeout\":5s,\"permitWithoutStream\":true}}",
			expectedOptions: &GRPCOptions{
				URL: "test",
				KeepAliveOptions: KeepAliveOptions{
					Enable:              true,
					Time:                10 * time.Second,
					Timeout:             5 * time.Second,
					PermitWithoutStream: true,
				},
			},
		},
		{
			name:   "customized options with ca",
			config: "{\"url\":\"test\",\"caFile\":\"test\"}",
			expectedOptions: &GRPCOptions{
				URL:    "test",
				CAFile: "test",
				KeepAliveOptions: KeepAliveOptions{
					Enable:              false,
					Time:                30 * time.Second,
					Timeout:             10 * time.Second,
					PermitWithoutStream: false,
				},
			},
		},
		{
			name:   "customized options with ca",
			config: "{\"url\":\"test\",\"caFile\":\"test\",\"clientCertFile\":\"test\",\"clientKeyFile\":\"test\"}",
			expectedOptions: &GRPCOptions{
				URL:            "test",
				CAFile:         "test",
				ClientCertFile: "test",
				ClientKeyFile:  "test",
				KeepAliveOptions: KeepAliveOptions{
					Enable:              false,
					Time:                30 * time.Second,
					Timeout:             10 * time.Second,
					PermitWithoutStream: false,
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := os.WriteFile(file.Name(), []byte(c.config), 0644); err != nil {
				t.Fatal(err)
			}

			options, err := BuildGRPCOptionsFromFlags(file.Name())
			if err != nil {
				if err.Error() != c.expectedErrorMsg {
					t.Errorf("unexpected err %v", err)
				}
			}

			if !reflect.DeepEqual(options, c.expectedOptions) {
				t.Errorf("unexpected options %v", options)
			}
		})
	}
}
