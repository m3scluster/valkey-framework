package main

import (
	"context"
	"testing"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
)

func TestSubscribedImmediatelyReconcilesPersistedTasks(t *testing.T) {
	caller := &recordingCaller{}
	s := testScheduler(caller)
	s.desired = true
	s.tasks = map[string]*Task{
		"master-1": {ID: "master-1", Role: "master", State: "TASK_RUNNING", Agent: "agent-1"},
		"slave-1":  {ID: "slave-1", Role: "slave-1", State: "TASK_RUNNING", Agent: "agent-2"},
	}

	event := &scheduler.Event{
		Type: scheduler.Event_SUBSCRIBED,
		Subscribed: &scheduler.Event_Subscribed{
			FrameworkID: lib.FrameworkID{Value: "framework-test"},
		},
	}
	if err := s.handleEvent(context.Background(), event); err != nil {
		t.Fatalf("handle SUBSCRIBED: %v", err)
	}

	callsSeen := caller.snapshot()
	if len(callsSeen) != 1 || callsSeen[0].GetType() != scheduler.Call_RECONCILE {
		t.Fatalf("calls = %#v, want one immediate RECONCILE", callsSeen)
	}
	if got := callsSeen[0].GetFrameworkID().GetValue(); got != "framework-test" {
		t.Fatalf("reconcile framework ID = %q, want framework-test", got)
	}
	seen := make(map[string]bool)
	for _, task := range callsSeen[0].GetReconcile().GetTasks() {
		seen[task.GetTaskID().Value] = true
	}
	if !seen["master-1"] || !seen["slave-1"] || len(seen) != 2 {
		t.Fatalf("reconciled tasks = %#v, want persisted task IDs", seen)
	}
}
