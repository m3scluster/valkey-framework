package main

import (
	"context"
	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
	"reflect"
	"testing"
	"time"
)

func TestSchedulerRegistrationTokensBackoffReconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tokens := schedulerRegistrationTokens(ctx)
	select {
	case <-tokens:
	default:
		t.Fatal("first registration must not be delayed")
	}
	select {
	case <-tokens:
		t.Fatal("re-registration must be rate-limited")
	case <-time.After(schedulerReconnectBackoff / 2):
	}
	select {
	case <-tokens:
	case <-time.After(schedulerReconnectBackoff + schedulerReconnectBackoff/2):
		t.Fatal("re-registration token was not released after backoff")
	}
}

func TestBuildTaskInfoUsesTypedMesosObjects(t *testing.T) {
	s := NewScheduler(Config{Name: "test", Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, CNI: "weave", MasterHost: "valkey-framework.mesos"})
	task := s.buildTaskInfo("master", "task-1", lib.Offer{AgentID: lib.AgentID{Value: "agent-1"}})
	if task.TaskID.Value != "task-1" || task.AgentID.Value != "agent-1" {
		t.Fatalf("unexpected task identity: %#v", task)
	}
	if task.Name != "master" || task.Container.GetHostname() != "master" {
		t.Fatalf("master task must use stable DNS name: %#v", task)
	}
	if len(task.Resources) != 2 || task.Container == nil || task.Container.Docker == nil || task.Container.Docker.Image != "valkey:test" {
		t.Fatalf("incomplete typed task: %#v", task)
	}
	if task.Container.Docker.GetNetwork() != lib.ContainerInfo_DockerInfo_USER || len(task.Container.NetworkInfos) != 1 || task.Container.NetworkInfos[0].GetName() != "weave" {
		t.Fatalf("missing CNI network: %#v", task.Container.NetworkInfos)
	}
}

func TestRecordFrameworkIDPersistsLatestSubscribedID(t *testing.T) {
	s := &Scheduler{}
	if !s.recordFrameworkID("framework-1") {
		t.Fatal("first SUBSCRIBED framework ID must be recorded")
	}
	if got := s.currentFrameworkID(); got != "framework-1" {
		t.Fatalf("recorded framework ID = %q, want framework-1", got)
	}
	if s.recordFrameworkID("framework-1") {
		t.Fatal("unchanged framework ID must not be reported as changed")
	}
}

func TestFrameworkErrorClearsStaleFrameworkID(t *testing.T) {
	s := &Scheduler{frameworkID: "removed-framework", framework: &lib.FrameworkInfo{ID: &lib.FrameworkID{Value: "removed-framework"}}}
	if err := s.handleEvent(context.Background(), &scheduler.Event{Type: scheduler.Event_ERROR, Error: &scheduler.Event_Error{Message: "Framework has been removed"}}); err != nil {
		t.Fatal(err)
	}
	if got := s.currentFrameworkID(); got != "" {
		t.Fatalf("framework ID after removal = %q, want empty", got)
	}
}

func TestRestoreStateRestoresFrameworkAndTasks(t *testing.T) {
	s := &Scheduler{tasks: map[string]*Task{}}
	b := []byte(`{"framework_id":"framework-redis","desired":true,"tasks":{"master-1":{"ID":"master-1","Role":"master","State":"TASK_RUNNING","Updated":"2026-01-01T00:00:00Z"}}}`)
	if !s.restoreState(b) {
		t.Fatal("valid persisted state must be restored")
	}
	if s.frameworkID != "framework-redis" || !s.desired {
		t.Fatalf("restored scheduler metadata = (%q, %v)", s.frameworkID, s.desired)
	}
	want := &Task{ID: "master-1", Role: "master", State: "TASK_RUNNING", Updated: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	if !reflect.DeepEqual(s.tasks["master-1"], want) {
		t.Fatalf("restored task = %#v, want %#v", s.tasks["master-1"], want)
	}
}

func TestRestoreStateRejectsInvalidJSON(t *testing.T) {
	s := &Scheduler{frameworkID: "keep", desired: true, tasks: map[string]*Task{"existing": {ID: "existing"}}}
	if s.restoreState([]byte("not-json")) {
		t.Fatal("invalid persisted state must be rejected")
	}
	if s.frameworkID != "keep" || !s.desired || len(s.tasks) != 1 {
		t.Fatalf("invalid state changed scheduler: %#v", s)
	}
}

func TestNextRole(t *testing.T) {
	// Test initial state - should return "master"
	s := NewScheduler(Config{Name: "test", Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, CNI: "weave", MasterHost: "valkey-framework.mesos", Slaves: 2})
	// No tasks at all - should return "master"
	next := s.nextRole()
	if next != "master" {
		t.Fatalf("Expected \"master\", got %q", next)
	}

	// Test TASK_STAGING state - should not return slave roles
	s.tasks["master-1"] = &Task{ID: "master-1", Role: "master", State: "TASK_STAGING"}
	next = s.nextRole()
	if next != "master" {
		t.Fatalf("Expected \"master\" when master is TASK_STAGING, got %q", next)
	}

	// Test TASK_STARTING state - should not return slave roles
	s.tasks["master-1"] = &Task{ID: "master-1", Role: "master", State: "TASK_STARTING"}
	next = s.nextRole()
	if next != "master" {
		t.Fatalf("Expected \"master\" when master is TASK_STARTING, got %q", next)
	}

	// Test TASK_UNKNOWN state - should not return slave roles
	s.tasks["master-1"] = &Task{ID: "master-1", Role: "master", State: "TASK_UNKNOWN"}
	next = s.nextRole()
	if next != "master" {
		t.Fatalf("Expected \"master\" when master is TASK_UNKNOWN, got %q", next)
	}

	// Test TASK_RUNNING state - should allow slave creation
	s.tasks["master-1"] = &Task{ID: "master-1", Role: "master", State: "TASK_RUNNING"}
	next = s.nextRole()
	if next != "slave-1" {
		t.Fatalf("Expected \"slave-1\" when master is TASK_RUNNING, got %q", next)
	}

	// Test with one slave already running - should return next slave
	s.tasks["slave-1"] = &Task{ID: "slave-1", Role: "slave-1", State: "TASK_RUNNING"}
	next = s.nextRole()
	if next != "slave-2" {
		t.Fatalf("Expected \"slave-2\" when one slave is running, got %q", next)
	}

	// Test with both slaves already running - should return empty string
	s.tasks["slave-2"] = &Task{ID: "slave-2", Role: "slave-2", State: "TASK_RUNNING"}
	next = s.nextRole()
	if next != "" {
		t.Fatalf("Expected \"\" when all slaves are running, got %q", next)
	}
}
