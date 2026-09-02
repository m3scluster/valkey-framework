package main

import (
	lib "github.com/mesos/mesos-go/api/v1/lib"
	"testing"
)

func TestBuildTaskInfoUsesTypedMesosObjects(t *testing.T) {
	s := NewScheduler(Config{Name: "test", Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, CNI: "weave", MasterHost: "valkey-framework.mesos"})
	task := s.buildTaskInfo("master", "task-1", lib.Offer{AgentID: lib.AgentID{Value: "agent-1"}})
	if task.TaskID.Value != "task-1" || task.AgentID.Value != "agent-1" {
		t.Fatalf("unexpected task identity: %#v", task)
	}
	if len(task.Resources) != 2 || task.Container == nil || task.Container.Docker == nil || task.Container.Docker.Image != "valkey:test" {
		t.Fatalf("incomplete typed task: %#v", task)
	}
	if len(task.Container.NetworkInfos) != 1 || task.Container.NetworkInfos[0].GetName() != "weave" {
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
