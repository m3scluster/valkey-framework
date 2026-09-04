package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler/calls"
)

func TestSchedulerHTTPTransport(t *testing.T) {
	t.Setenv("MESOS_USERNAME", "test-user")
	requests := make(chan struct {
		call scheduler.Call
		auth string
	}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/scheduler" {
			t.Errorf("path = %s, want /api/v1/scheduler", r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); !strings.Contains(got, "application/json") {
			t.Errorf("content type = %q, want application/json", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		var call scheduler.Call
		if err := json.Unmarshal(body, &call); err != nil {
			t.Errorf("decode scheduler call: %v", err)
		}
		requests <- struct {
			call scheduler.Call
			auth string
		}{call: call, auth: r.Header.Get("Authorization")}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Mesos-Stream-Id", "stream-test")
		_, _ = w.Write([]byte(`{"type":"SUBSCRIBED","subscribed":{"framework_id":{"value":"framework-test"}}}`))
	}))
	defer server.Close()

	s := NewScheduler(Config{
		Master:      server.URL,
		Name:        "test-framework",
		User:        "test-user",
		Role:        "*",
		Password:    "test-password",
		RedisServer: "127.0.0.1:1",
		FrontendURL: "https://frontend.example:5173",
	})
	response, err := s.caller.Call(context.Background(), calls.Subscribe(s.framework))
	if err != nil {
		t.Fatalf("SUBSCRIBE transport: %v", err)
	}
	if response == nil {
		t.Fatal("SUBSCRIBE returned nil response")
	}
	var event scheduler.Event
	if err := response.Decode(&event); err != nil {
		t.Fatalf("decode SUBSCRIBED response: %v", err)
	}
	if err := response.Close(); err != nil {
		t.Fatalf("close response: %v", err)
	}

	request := <-requests
	if request.call.GetType() != scheduler.Call_SUBSCRIBE {
		t.Fatalf("request type = %s, want SUBSCRIBE", request.call.GetType())
	}
	if request.call.GetSubscribe().GetFrameworkInfo().GetName() != "test-framework" {
		t.Fatalf("framework name = %q, want test-framework", request.call.GetSubscribe().GetFrameworkInfo().GetName())
	}
	if got := request.call.GetSubscribe().GetFrameworkInfo().GetWebUiURL(); got != "https://frontend.example:5173" {
		t.Fatalf("framework web UI URL = %q, want https://frontend.example:5173", got)
	}
	if request.auth == "" {
		t.Fatal("expected Basic Auth header")
	}
	frameworkID := event.GetSubscribed().GetFrameworkID()
	if event.GetType() != scheduler.Event_SUBSCRIBED || frameworkID.Value != "framework-test" {
		t.Fatalf("response event = %#v, want SUBSCRIBED framework-test", event)
	}
}

func TestLiveSmoke(t *testing.T) {
	master := strings.TrimSpace(getenvForTest("MESOS_MASTER"))
	password := getenvForTest("MESOS_PASSWORD")
	if master == "" {
		t.Skip("live smoke skipped: MESOS_MASTER is not set")
	}
	if password == "" {
		t.Skip("live smoke skipped: MESOS_PASSWORD is not set")
	}

	t.Setenv("MESOS_MASTER", master)
	t.Setenv("MESOS_PASSWORD", password)
	if getenvForTest("MESOS_USERNAME") == "" {
		t.Setenv("MESOS_USERNAME", getenvForTest("USER"))
	}
	config := loadConfig()
	s := NewScheduler(config)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response, err := s.caller.Call(ctx, calls.Subscribe(s.framework))
	if err != nil {
		t.Fatalf("live SUBSCRIBE against %s: %v", config.Master, err)
	}
	if response == nil {
		t.Fatal("live SUBSCRIBE returned nil response")
	}
	_ = response.Close()
	t.Logf("live scheduler SUBSCRIBE succeeded against %s", config.Master)
}

func getenvForTest(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}
