package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
)

func TestStatusDoesNotTreatPersistedTasksAsConnected(t *testing.T) {
	s := &Scheduler{
		frameworkID: "persisted-framework",
		desired:     true,
		tasks: map[string]*Task{
			"master-1": {ID: "master-1", Role: "master", State: "TASK_RUNNING"},
		},
	}

	recorder := httptest.NewRecorder()
	s.handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	var status struct {
		Desired            bool `json:"desired"`
		SchedulerConnected bool `json:"scheduler_connected"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if !status.Desired || status.SchedulerConnected {
		t.Fatalf("status = %#v, want desired=true and scheduler_connected=false", status)
	}
}

func TestSubscribedMarksSchedulerConnected(t *testing.T) {
	s := testScheduler(&recordingCaller{})
	if err := s.handleEvent(context.Background(), &scheduler.Event{
		Type: scheduler.Event_SUBSCRIBED,
		Subscribed: &scheduler.Event_Subscribed{
			FrameworkID: lib.FrameworkID{Value: "framework-live"},
		},
	}); err != nil {
		t.Fatalf("handle SUBSCRIBED: %v", err)
	}
	if !s.schedulerConnected {
		t.Fatal("SUBSCRIBED must mark scheduler connected")
	}
}

func TestMesosErrorMarksSchedulerDisconnected(t *testing.T) {
	s := &Scheduler{frameworkID: "framework-live", schedulerConnected: true, framework: &lib.FrameworkInfo{ID: &lib.FrameworkID{Value: "framework-live"}}}
	if err := s.handleEvent(context.Background(), &scheduler.Event{
		Type:  scheduler.Event_ERROR,
		Error: &scheduler.Event_Error{Message: "framework removed"},
	}); err != nil {
		t.Fatalf("handle ERROR: %v", err)
	}
	if s.schedulerConnected {
		t.Fatal("Mesos ERROR must mark scheduler disconnected")
	}
}

func TestMetricsDoesNotReportPersistedTasksWhileDisconnected(t *testing.T) {
	s := &Scheduler{desired: true, tasks: map[string]*Task{
		"master-1": {ID: "master-1", Role: "master", State: "TASK_RUNNING"},
	}}
	recorder := httptest.NewRecorder()
	s.handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/metrics", nil))
	var metrics struct {
		Total   int `json:"total"`
		Running int `json:"running"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &metrics); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	if metrics.Total != 0 || metrics.Running != 0 {
		t.Fatalf("metrics = %#v, want no live tasks while disconnected", metrics)
	}
}
