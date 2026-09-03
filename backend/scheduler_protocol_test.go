package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler/calls"
)

func TestSchedulerClientDecodesOpenJSONRecordIOStream(t *testing.T) {
	payload := `{"type":"SUBSCRIBED","subscribed":{"framework_id":{"value":"framework-test"}}}`
	requestSeen := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestSeen <- r
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Mesos-Stream-Id", "stream-test")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "%d\n%s", len(payload), payload)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	s := NewScheduler(Config{Master: server.URL, Name: "test", User: "test", Role: "valkey", Image: "valkey:test"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := s.caller.Call(ctx, calls.Subscribe(&lib.FrameworkInfo{Name: "test", User: "test"}))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer resp.Close()
	var event scheduler.Event
	if err := resp.Decode(&event); err != nil {
		t.Fatalf("decode subscribed: %v", err)
	}
	if event.GetType() != scheduler.Event_SUBSCRIBED || event.GetSubscribed().FrameworkID.GetValue() != "framework-test" {
		t.Fatalf("event=%+v", event)
	}
	select {
	case req := <-requestSeen:
		if req.Method != http.MethodPost || req.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("request method/content-type=%s/%s", req.Method, req.Header.Get("Content-Type"))
		}
	case <-ctx.Done():
		t.Fatal("request was not received")
	}
	if strings.Contains(s.currentFrameworkID(), "context") {
		t.Fatal("context leaked into framework ID")
	}
}
