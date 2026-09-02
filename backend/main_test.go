package main

import (
	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
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
