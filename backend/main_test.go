package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
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

func TestBuildTaskInfoWithoutCNIUsesHostNetworking(t *testing.T) {
	s := NewScheduler(Config{Name: "test", Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, CNI: "", MasterHost: "master.test.mesos"})
	task := s.buildTaskInfo("master", "task-1", lib.Offer{AgentID: lib.AgentID{Value: "agent-1"}})
	if task.Container.Docker.GetNetwork() != lib.ContainerInfo_DockerInfo_HOST || len(task.Container.NetworkInfos) != 0 {
		t.Fatalf("no-CNI task must use host networking without NetworkInfos: %#v", task.Container)
	}
}

func TestLoadConfigUsesConfigurableDomain(t *testing.T) {
	t.Setenv("FRAMEWORK_NAME", "valkey-test")
	t.Setenv("MESOS_DOMAIN", "cluster.internal")
	t.Setenv("MESOS_CNI", "")
	t.Setenv("VALKEY_SLAVES", "0")
	c := loadConfig()
	if c.CNI != "" {
		t.Fatalf("CNI = %q, want empty", c.CNI)
	}
	if c.Slaves != minSlaves {
		t.Fatalf("Slaves = %d, want minimum %d", c.Slaves, minSlaves)
	}
	if c.Domain != "cluster.internal" {
		t.Fatalf("Domain = %q, want cluster.internal", c.Domain)
	}
	if c.MasterHost != "master.valkey-test.cluster.internal" {
		t.Fatalf("MasterHost = %q, want master.valkey-test.cluster.internal", c.MasterHost)
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
	s := &Scheduler{tasks: map[string]*Task{}, framework: &lib.FrameworkInfo{}}
	b := []byte(`{"framework_id":"framework-redis","desired":true,"tasks":{"master-1":{"ID":"master-1","Role":"master","State":"TASK_RUNNING","Updated":"2026-01-01T00:00:00Z"}}}`)
	if !s.restoreState(b) {
		t.Fatal("valid persisted state must be restored")
	}
	if s.frameworkID != "framework-redis" || !s.desired {
		t.Fatalf("restored scheduler metadata = (%q, %v)", s.frameworkID, s.desired)
	}
	if got := s.framework.GetID().GetValue(); got != "framework-redis" {
		t.Fatalf("restored FrameworkInfo ID = %q, want framework-redis", got)
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

func TestMetricsEndpointCountsTaskStates(t *testing.T) {
	s := &Scheduler{frameworkID: "framework-1", desired: true, tasks: map[string]*Task{
		"running": {State: "TASK_RUNNING"},
		"staging": {State: "TASK_STAGING"},
		"failed":  {State: "TASK_FAILED"},
		"lost":    {State: "TASK_LOST"},
	}}
	recording := httptest.NewRecorder()
	s.handler().ServeHTTP(recording, httptest.NewRequest("GET", "/api/metrics", nil))
	if recording.Code != 200 {
		t.Fatalf("metrics status = %d, want 200", recording.Code)
	}
	var got struct {
		Total   int `json:"total"`
		Running int `json:"running"`
		Staging int `json:"staging"`
		Failed  int `json:"failed"`
	}
	if err := json.Unmarshal(recording.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	if got.Total != 4 || got.Running != 1 || got.Staging != 1 || got.Failed != 2 {
		t.Fatalf("metrics = %#v, want total=4 running=1 staging=1 failed=2", got)
	}
}

func TestScaleEndpointValidatesAndPersistsTarget(t *testing.T) {
	stateFile := t.TempDir() + "/state.json"
	s := &Scheduler{cfg: Config{Name: "test", Slaves: 2, StateFile: stateFile}, tasks: map[string]*Task{}}
	handler := s.handler()

	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, httptest.NewRequest("POST", "/api/scale", strings.NewReader(`{"slaves":0}`)))
	if invalid.Code != http.StatusBadRequest || s.cfg.Slaves != 2 {
		t.Fatalf("invalid scale: status=%d slaves=%d", invalid.Code, s.cfg.Slaves)
	}

	valid := httptest.NewRecorder()
	handler.ServeHTTP(valid, httptest.NewRequest("POST", "/api/scale", strings.NewReader(`{"slaves":4}`)))
	if valid.Code != http.StatusAccepted || s.cfg.Slaves != 4 {
		t.Fatalf("valid scale: status=%d slaves=%d", valid.Code, s.cfg.Slaves)
	}
	persisted, err := os.ReadFile(stateFile)
	if err != nil || !strings.Contains(string(persisted), `"Slaves":4`) {
		t.Fatalf("scaled target was not persisted: err=%v state=%s", err, persisted)
	}

	status := httptest.NewRecorder()
	handler.ServeHTTP(status, httptest.NewRequest("GET", "/api/status", nil))
	var got struct {
		Masters   int `json:"masters"`
		Slaves    int `json:"slaves"`
		MinSlaves int `json:"min_slaves"`
	}
	if err := json.Unmarshal(status.Body.Bytes(), &got); err != nil || got.Masters != 1 || got.Slaves != 4 || got.MinSlaves != minSlaves {
		t.Fatalf("status scaling metadata = %#v, err=%v", got, err)
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
