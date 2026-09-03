package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/extras/scheduler/controller"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler/calls"
	"github.com/redis/go-redis/v9"
)

type recordingCaller struct {
	mu    sync.Mutex
	calls []*scheduler.Call
}

func (c *recordingCaller) Call(_ context.Context, call *scheduler.Call) (lib.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
	return nil, nil
}

func (c *recordingCaller) snapshot() []*scheduler.Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*scheduler.Call, len(c.calls))
	copy(result, c.calls)
	return result
}

func testScheduler(caller calls.Caller) *Scheduler {
	return &Scheduler{
		cfg:    Config{CPU: 0.2, Memory: 128, Image: "valkey:test", CNI: "weave", MasterHost: "valkey-framework.mesos", Port: 6379, Slaves: 1, StateFile: "/tmp/valkey-mesos-lifecycle-test.json"},
		tasks:  map[string]*Task{},
		state:  redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
		caller: caller,
	}
}

func TestSchedulerLifecycleEvents(t *testing.T) {
	caller := &recordingCaller{}
	s := testScheduler(caller)

	t.Run("SUBSCRIBE/SUBSCRIBED records framework ID", func(t *testing.T) {
		framework := &lib.FrameworkInfo{User: "test-user", Name: "test-framework"}
		subscribe := calls.Subscribe(framework)
		if _, err := caller.Call(context.Background(), subscribe); err != nil {
			t.Fatalf("record SUBSCRIBE: %v", err)
		}
		if subscribe.GetType() != scheduler.Call_SUBSCRIBE || subscribe.GetSubscribe().GetFrameworkInfo().GetName() != "test-framework" {
			t.Fatalf("subscribe call = %#v, want SUBSCRIBE for test-framework", subscribe)
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
		if got := s.currentFrameworkID(); got != "framework-test" {
			t.Fatalf("framework ID = %q, want framework-test", got)
		}
	})

	t.Run("OFFERS accepts an offer and tracks launched task", func(t *testing.T) {
		s.desired = true
		offer := lib.Offer{
			ID:        lib.OfferID{Value: "offer-1"},
			AgentID:   lib.AgentID{Value: "agent-1"},
			Hostname:  "agent.example",
			Resources: []lib.Resource{scalar("cpus", 1), scalar("mem", 512)},
		}
		if err := s.handleEvent(context.Background(), &scheduler.Event{
			Type:   scheduler.Event_OFFERS,
			Offers: &scheduler.Event_Offers{Offers: []lib.Offer{offer}},
		}); err != nil {
			t.Fatalf("handle OFFERS: %v", err)
		}

		callsSeen := caller.snapshot()
		if len(callsSeen) != 2 {
			t.Fatalf("caller received %d calls, want SUBSCRIBE and ACCEPT", len(callsSeen))
		}
		accept := callsSeen[1]
		if accept.GetType() != scheduler.Call_ACCEPT || accept.GetAccept() == nil {
			t.Fatalf("call = %#v, want ACCEPT", accept)
		}
		if len(accept.GetAccept().GetOfferIDs()) != 1 || accept.GetAccept().GetOfferIDs()[0].Value != "offer-1" {
			t.Fatalf("accepted offers = %#v, want offer-1", accept.GetAccept().GetOfferIDs())
		}
		if len(accept.GetAccept().GetOperations()) != 1 || accept.GetAccept().GetOperations()[0].GetType() != lib.Offer_Operation_LAUNCH {
			t.Fatalf("operations = %#v, want one LAUNCH", accept.GetAccept().GetOperations())
		}
		if len(s.tasks) != 1 {
			t.Fatalf("tracked tasks = %d, want 1", len(s.tasks))
		}
	})

	t.Run("UPDATE changes tracked task state", func(t *testing.T) {
		var taskID string
		for taskID = range s.tasks {
			break
		}
		state := lib.TASK_RUNNING
		if err := s.handleEvent(context.Background(), &scheduler.Event{
			Type:   scheduler.Event_UPDATE,
			Update: &scheduler.Event_Update{Status: lib.TaskStatus{TaskID: lib.TaskID{Value: taskID}, State: &state}},
		}); err != nil {
			t.Fatalf("handle UPDATE: %v", err)
		}
		if got := s.tasks[taskID].State; got != state.String() {
			t.Fatalf("task state = %q, want %q", got, state.String())
		}
	})
}

func TestAckStatusUpdateSendsAcknowledgeCall(t *testing.T) {
	caller := &recordingCaller{}
	state := lib.TASK_RUNNING
	event := &scheduler.Event{
		Type: scheduler.Event_UPDATE,
		Update: &scheduler.Event_Update{Status: lib.TaskStatus{
			TaskID: lib.TaskID{Value: "task-1"}, AgentID: &lib.AgentID{Value: "agent-1"}, State: &state, UUID: []byte{1, 2, 3},
		}},
	}
	if err := controller.AckStatusUpdates(caller).HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("ack rule: %v", err)
	}

	callsSeen := caller.snapshot()
	if len(callsSeen) != 1 {
		t.Fatalf("caller received %d calls, want 1", len(callsSeen))
	}
	ack := callsSeen[0]
	if ack.GetType() != scheduler.Call_ACKNOWLEDGE || ack.GetAcknowledge() == nil {
		t.Fatalf("call = %#v, want ACKNOWLEDGE", ack)
	}
	if got := ack.GetAcknowledge().GetAgentID().Value; got != "agent-1" {
		t.Fatalf("ack agent ID = %q, want agent-1", got)
	}
	if got := ack.GetAcknowledge().GetTaskID().Value; got != "task-1" {
		t.Fatalf("ack task ID = %q, want task-1", got)
	}
	if got := ack.GetAcknowledge().GetUUID(); string(got) != string([]byte{1, 2, 3}) {
		t.Fatalf("ack UUID = %v, want [1 2 3]", got)
	}
}

func TestTerminalStatusUpdateRemovesTaskAndPersistsCleanup(t *testing.T) {
	caller := &recordingCaller{}
	stateFile := t.TempDir() + "/state.json"
	s := testScheduler(caller)
	s.cfg.StateFile = stateFile
	s.tasks["slave-1"] = &Task{ID: "slave-1", Role: "slave-1", State: "TASK_RUNNING"}
	state := lib.TASK_LOST

	if err := s.handleEvent(context.Background(), &scheduler.Event{
		Type: scheduler.Event_UPDATE,
		Update: &scheduler.Event_Update{Status: lib.TaskStatus{
			TaskID:  lib.TaskID{Value: "slave-1"},
			AgentID: &lib.AgentID{Value: "agent-1"},
			State:   &state,
		}},
	}); err != nil {
		t.Fatalf("handle terminal UPDATE: %v", err)
	}
	if _, ok := s.tasks["slave-1"]; ok {
		t.Fatal("terminal task must be removed from the in-memory task set")
	}

	persisted, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read persisted state: %v", err)
	}
	var snapshot struct {
		Tasks map[string]*Task `json:"tasks"`
	}
	if err := json.Unmarshal(persisted, &snapshot); err != nil {
		t.Fatalf("decode persisted state: %v", err)
	}
	if len(snapshot.Tasks) != 0 {
		t.Fatalf("persisted terminal tasks = %#v, want empty", snapshot.Tasks)
	}
}
