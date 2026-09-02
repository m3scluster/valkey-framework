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
