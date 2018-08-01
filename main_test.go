package main

import (
	"reflect"
	"testing"
)

func TestNormalizeNatsServers(t *testing.T) {
	testCases := []struct {
		testCase    string
		natsServers []string
		want        []string
	}{
		{
			"No servers (should not happen but test it anyway)",
			nil,
			nil,
		},
		{
			"Single server no spaces",
			[]string{"nats://localhost:9876"},
			[]string{"nats://localhost:9876"},
		},
		{
			"Single server with spaces",
			[]string{" nats://localhost:9876  "},
			[]string{"nats://localhost:9876"},
		},
		{
			"2 instances of --nats-servers",
			[]string{"nats://localhost:9876", "nats://localhost:6789"},
			[]string{"nats://localhost:9876", "nats://localhost:6789"},
		},
		{
			"2 instances of --nats-servers with leading+trailing spaces",
			[]string{" nats://localhost:9876    ", "  nats://localhost:6789 "},
			[]string{"nats://localhost:9876", "nats://localhost:6789"},
		},
		{
			"Single instance of --nats-servers with multiple servers, no spaces",
			[]string{"nats://localhost:3434,nats://localhost:2121"},
			[]string{"nats://localhost:3434", "nats://localhost:2121"},
		},
		{
			"Single instance of --nats-servers with multiple servers and spaces",
			[]string{"  nats://localhost:1111,    nats://localhost:2222  ,nats://localhost:3333"},
			[]string{"nats://localhost:1111", "nats://localhost:2222", "nats://localhost:3333"},
		},
		{
			"Multiple instances of --nats-servers with multiple servers and spaces",
			[]string{"nats://localhost:9999", " nats://localhost:8888  ,   nats://localhost:7777  ,nats://localhost:6666 ", "    nats://localhost:5555        "},
			[]string{"nats://localhost:9999", "nats://localhost:8888", "nats://localhost:7777", "nats://localhost:6666", "nats://localhost:5555"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testCase, func(t *testing.T) {
			natsServers, err := normalizeNatsServers(tc.natsServers)
			if err != nil {
				t.Fatalf("returned error: %#v", tc)
			}
			if !reflect.DeepEqual(natsServers, tc.want) {
				t.Fatalf("\n  wanted: %#v\n     got: %#v", tc.want, natsServers)
			}
		})
	}
}
