package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
	logrus "github.com/sirupsen/logrus"
)

type startupLogHook struct {
	entry *logrus.Entry
}

func (h *startupLogHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *startupLogHook) Fire(entry *logrus.Entry) error {
	h.entry = entry
	return nil
}

func TestStartupLogMetadata(t *testing.T) {
	logger := logrus.StandardLogger()
	oldHooks := logger.Hooks
	logger.ReplaceHooks(make(logrus.LevelHooks))
	defer logger.ReplaceHooks(oldHooks)
	hook := &startupLogHook{}
	logger.AddHook(hook)

	logStartup(Config{Master: "https://mesos.example:5050"})
	if hook.entry == nil {
		t.Fatal("startup log entry was not emitted")
	}
	if hook.entry.Message != "scheduler starting" {
		t.Fatalf("startup log message = %q, want scheduler starting", hook.entry.Message)
	}
	if got := hook.entry.Data["program"]; got != programName {
		t.Fatalf("program metadata = %v, want %q", got, programName)
	}
	if got := hook.entry.Data["version"]; got != version || got == "" {
		t.Fatalf("version metadata = %v, want non-empty build version %q", got, version)
	}
	if got := hook.entry.Data["mesos_master"]; got != "https://mesos.example:5050" {
		t.Fatalf("Mesos master metadata = %v, want configured URL", got)
	}
}

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
	s := NewScheduler(Config{Name: "test", Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, CNI: "weave", Domain: "weave.local", MasterHost: "master.weave.local"})
	task := s.buildTaskInfo("master", "task-1", lib.Offer{AgentID: lib.AgentID{Value: "agent-1"}})
	if task.TaskID.Value != "task-1" || task.AgentID.Value != "agent-1" {
		t.Fatalf("unexpected task identity: %#v", task)
	}
	if task.Name != "master" || task.Container.GetHostname() != "master.weave.local" {
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
	s := NewScheduler(Config{Name: "test", Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, CNI: "", Domain: "mesos", MasterHost: "master.mesos"})
	task := s.buildTaskInfo("master", "task-1", lib.Offer{AgentID: lib.AgentID{Value: "agent-1"}})
	if task.Container.Docker.GetNetwork() != lib.ContainerInfo_DockerInfo_HOST || len(task.Container.NetworkInfos) != 0 {
		t.Fatalf("no-CNI task must use host networking without NetworkInfos: %#v", task.Container)
	}
}

func TestBuildTaskInfoUsesConfiguredDomainForHostname(t *testing.T) {
	s := NewScheduler(Config{Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, Domain: ".weave.local."})
	task := s.buildTaskInfo("slave-1", "slave-1-123", lib.Offer{AgentID: lib.AgentID{Value: "agent-1"}})
	if task.Container.GetHostname() != "slave-1-123.weave.local" {
		t.Fatalf("task hostname = %q, want slave-1-123.weave.local", task.Container.GetHostname())
	}
}

func TestLoadConfigUsesConfigurableDomain(t *testing.T) {
	t.Setenv("FRAMEWORK_NAME", "valkey-test")
	t.Setenv("MESOS_DOMAIN", "cluster.internal")
	t.Setenv("MESOS_CNI", "")
	t.Setenv("VALKEY_MASTERS", "0")
	t.Setenv("VALKEY_SLAVES", "0")
	c := loadConfig()
	if c.CNI != "" {
		t.Fatalf("CNI = %q, want empty", c.CNI)
	}
	if c.Masters != minMasters {
		t.Fatalf("Masters = %d, want minimum %d", c.Masters, minMasters)
	}
	if c.Slaves != minSlaves {
		t.Fatalf("Slaves = %d, want minimum %d", c.Slaves, minSlaves)
	}
	if c.Domain != "cluster.internal" {
		t.Fatalf("Domain = %q, want cluster.internal", c.Domain)
	}
	if c.MasterHost != "master.cluster.internal" {
		t.Fatalf("MasterHost = %q, want master.cluster.internal", c.MasterHost)
	}
}

func TestLoadConfigDoesNotInferDomainFromCNI(t *testing.T) {
	t.Setenv("MESOS_CNI", "weave")
	t.Setenv("MESOS_DOMAIN", "")
	c := loadConfig()
	if c.Domain != "mesos" || c.MasterHost != "master.mesos" {
		t.Fatalf("CNI-derived domain = (%q, %q), want (mesos, master.mesos)", c.Domain, c.MasterHost)
	}
}

func TestLoadConfigUsesFrontendURL(t *testing.T) {
	t.Setenv("FRONTEND_URL", "")
	c := loadConfig()
	if c.FrontendURL != "http://localhost:5173" {
		t.Fatalf("FrontendURL = %q, want http://localhost:5173 (default)", c.FrontendURL)
	}

	t.Setenv("FRONTEND_URL", "https://admin.example.com")
	c = loadConfig()
	if c.FrontendURL != "https://admin.example.com" {
		t.Fatalf("FrontendURL = %q, want https://admin.example.com (from env)", c.FrontendURL)
	}
}

func TestLoadConfigParsesRedisPoolSize(t *testing.T) {
	t.Setenv("REDIS_SERVER", "redis.example:6380")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_DB", "7")
	t.Setenv("REDIS_POOLSIZE", "10")
	c := loadConfig()

	if c.RedisServer != "redis.example:6380" {
		t.Fatalf("RedisServer = %q, want redis.example:6380", c.RedisServer)
	}
	if c.RedisPassword != "secret" {
		t.Fatalf("RedisPassword = %q, want secret", c.RedisPassword)
	}
	if c.RedisDB != 7 {
		t.Fatalf("RedisDB = %d, want 7", c.RedisDB)
	}
	if c.RedisPoolSize != 10 {
		t.Fatalf("RedisPoolSize = %d, want 10", c.RedisPoolSize)
	}
}

func TestLoadConfigUsesRedisDefaults(t *testing.T) {
	for _, key := range []string{"REDIS_SERVER", "REDIS_PASSWORD", "REDIS_DB", "REDIS_POOLSIZE"} {
		t.Setenv(key, "")
	}
	c := loadConfig()
	if c.RedisServer != "redis.weave.local:6379" || c.RedisPassword != "" || c.RedisDB != 10 || c.RedisPoolSize != 0 {
		t.Fatalf("Redis config defaults = (%q, %q, %d, %d), want (redis.weave.local:6379, empty, 10, 0)", c.RedisServer, c.RedisPassword, c.RedisDB, c.RedisPoolSize)
	}
}

func TestNewSchedulerSetsFrameworkInfoWebUiURL(t *testing.T) {
	c := Config{Name: "test", Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, CNI: "weave", Domain: "weave.local", MasterHost: "master.weave.local", FrontendURL: "https://custom.example.com"}
	s := NewScheduler(c)
	if s.framework.WebUiURL == nil {
		t.Fatal("FrameworkInfo WebUiURL should be set with custom value")
	}
	if *s.framework.WebUiURL != "https://custom.example.com" {
		t.Fatalf("FrameworkInfo WebUiURL = %q, want https://custom.example.com (custom)", *s.framework.WebUiURL)
	}

	c.FrontendURL = ""
	s = NewScheduler(c)
	if s.framework.WebUiURL != nil {
		t.Fatalf("FrameworkInfo WebUiURL should be nil when empty string passed")
	}
}

func TestNewSchedulerSetsFrameworkInfoCheckpoint(t *testing.T) {
	c := Config{Name: "test", Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, CNI: "weave", Domain: "weave.local", MasterHost: "master.weave.local"}
	s := NewScheduler(c)
	if s.framework.Checkpoint == nil {
		t.Fatal("FrameworkInfo Checkpoint should be set")
	}
	if *s.framework.Checkpoint != false {
		t.Fatalf("FrameworkInfo Checkpoint = %v, want false (explicit Config default)", *s.framework.Checkpoint)
	}

	// Test explicitly enabled checkpointing
	c.Checkpoint = true
	s = NewScheduler(c)
	if s.framework.Checkpoint == nil {
		t.Fatal("FrameworkInfo Checkpoint should be set")
	}
	if *s.framework.Checkpoint != true {
		t.Fatalf("FrameworkInfo Checkpoint = %v, want true (explicitly set)", *s.framework.Checkpoint)
	}
}

func TestLoadConfigEnablesCheckpointingByDefault(t *testing.T) {
	t.Setenv("MESOS_CHECKPOINT", "")
	if got := loadConfig().Checkpoint; !got {
		t.Fatal("checkpointing should be enabled by default")
	}

	t.Setenv("MESOS_CHECKPOINT", "false")
	if got := loadConfig().Checkpoint; got {
		t.Fatal("MESOS_CHECKPOINT=false should disable checkpointing")
	}
}

func TestNewRedisClientUsesRedisConfig(t *testing.T) {
	client := newRedisClient(Config{
		Name:          "test",
		RedisServer:   "redis.example:6380",
		RedisPassword: "secret",
		RedisDB:       7,
		RedisPoolSize: 10,
	})
	defer client.Close()
	options := client.Options()
	if options.Addr != "redis.example:6380" || options.Password != "secret" || options.DB != 7 || options.PoolSize != 10 {
		t.Fatalf("Redis client options = (addr=%q, password=%q, db=%d, pool=%d), want configured values", options.Addr, options.Password, options.DB, options.PoolSize)
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

type fakeValkeyMetricsReader struct {
	info       string
	err        error
	sections   []string
	reads      int
	duringRead func()
}

func (f *fakeValkeyMetricsReader) Info(_ context.Context, sections ...string) (string, error) {
	f.reads++
	f.sections = append([]string(nil), sections...)
	if f.duringRead != nil {
		f.duringRead()
	}
	return f.info, f.err
}

func TestMetricsEndpointReadsAndParsesAllValkeyInfoSectionsWithoutSchedulerLock(t *testing.T) {
	reader := &fakeValkeyMetricsReader{info: `# Memory
used_memory:1048576
used_memory_human:1.00M
malformed
# Clients
connected_clients:12
# Stats
instantaneous_ops_per_sec:42
# Replication
role:master
connected_slaves:2
# CPU
used_cpu_sys:3.25
# Commandstats
cmdstat_get:calls=8,usec=16,usec_per_call=2.00
# Latencystats
latency_percentiles_usec_get:p50=1.003,p99=4.015
`}
	s := &Scheduler{frameworkID: "framework-1", schedulerConnected: true, desired: true, tasks: map[string]*Task{
		"running": {State: "TASK_RUNNING"},
	}, metricsReader: reader}
	reader.duringRead = func() {
		if !s.mu.TryLock() {
			t.Fatal("Scheduler.mu was held during Valkey INFO call")
		}
		s.mu.Unlock()
	}

	recording := httptest.NewRecorder()
	s.handler().ServeHTTP(recording, httptest.NewRequest("GET", "/api/metrics", nil))
	if recording.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", recording.Code)
	}
	wantSections := []string{"memory", "clients", "stats", "replication", "cpu", "commandstats", "latencystats"}
	if !reflect.DeepEqual(reader.sections, wantSections) {
		t.Fatalf("INFO sections = %#v, want %#v", reader.sections, wantSections)
	}
	var got struct {
		Total  int `json:"total"`
		Valkey struct {
			Available bool                      `json:"available"`
			Error     string                    `json:"error"`
			Sections  map[string]map[string]any `json:"sections"`
		} `json:"valkey"`
	}
	if err := json.Unmarshal(recording.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	if !got.Valkey.Available || got.Valkey.Error != "" || got.Total != 1 {
		t.Fatalf("metrics availability/backward fields = %#v", got)
	}
	if got.Valkey.Sections["memory"]["used_memory"] != float64(1048576) || got.Valkey.Sections["clients"]["connected_clients"] != float64(12) || got.Valkey.Sections["cpu"]["used_cpu_sys"] != 3.25 {
		t.Fatalf("numeric INFO values not decoded as JSON numbers: %#v", got.Valkey.Sections)
	}
	if got.Valkey.Sections["memory"]["used_memory_human"] != "1.00M" || got.Valkey.Sections["commandstats"]["cmdstat_get"] != "calls=8,usec=16,usec_per_call=2.00" || got.Valkey.Sections["latencystats"]["latency_percentiles_usec_get"] != "p50=1.003,p99=4.015" {
		t.Fatalf("text/compound INFO values not preserved: %#v", got.Valkey.Sections)
	}
	for _, section := range wantSections {
		if got.Valkey.Sections[section] == nil {
			t.Fatalf("missing section %q in %#v", section, got.Valkey.Sections)
		}
	}
}

func TestMetricsEndpointDoesNotReadValkeyBeforeNodeStarts(t *testing.T) {
	reader := &fakeValkeyMetricsReader{}
	s := &Scheduler{desired: true, tasks: map[string]*Task{}, metricsReader: reader}

	recording := httptest.NewRecorder()
	s.handler().ServeHTTP(recording, httptest.NewRequest("GET", "/api/metrics", nil))

	if reader.reads != 0 {
		t.Fatalf("Valkey INFO reads before a node starts = %d, want 0", reader.reads)
	}
	var got struct {
		Valkey struct {
			Available bool   `json:"available"`
			Error     string `json:"error"`
		} `json:"valkey"`
	}
	if err := json.Unmarshal(recording.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	if got.Valkey.Available || got.Valkey.Error != "Valkey nodes are not running" {
		t.Fatalf("metrics before node start = %#v, want unavailable without an INFO read", got.Valkey)
	}
}

func TestMetricsEndpointCountsTaskStates(t *testing.T) {
	s := &Scheduler{frameworkID: "framework-1", schedulerConnected: true, desired: true, tasks: map[string]*Task{
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
	if got.Total != 2 || got.Running != 1 || got.Staging != 1 || got.Failed != 2 {
		t.Fatalf("metrics = %#v, want live total=2 running=1 staging=1 failed=2", got)
	}
}

func TestScaleEndpointValidatesAndUpdatesTarget(t *testing.T) {
	s := &Scheduler{cfg: Config{Name: "test", Slaves: 2}, tasks: map[string]*Task{}}
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
	status := httptest.NewRecorder()
	handler.ServeHTTP(status, httptest.NewRequest("GET", "/api/status", nil))
	var got struct {
		Masters    int `json:"masters"`
		Slaves     int `json:"slaves"`
		MinMasters int `json:"min_masters"`
		MinSlaves  int `json:"min_slaves"`
	}
	if err := json.Unmarshal(status.Body.Bytes(), &got); err != nil || got.Masters != 1 || got.Slaves != 4 || got.MinMasters != minMasters || got.MinSlaves != minSlaves {
		t.Fatalf("status scaling metadata = %#v, err=%v", got, err)
	}
}

func TestScaleDownKillsSurplusSlaves(t *testing.T) {
	caller := &recordingCaller{}
	s := &Scheduler{
		cfg:         Config{Name: "test", Slaves: 3},
		frameworkID: "framework-test",
		tasks: map[string]*Task{
			"slave-1-task": {ID: "slave-1-task", Role: "slave-1", State: "TASK_RUNNING", Agent: "agent-1"},
			"slave-2-task": {ID: "slave-2-task", Role: "slave-2", State: "TASK_RUNNING", Agent: "agent-2"},
			"slave-3-task": {ID: "slave-3-task", Role: "slave-3", State: "TASK_RUNNING", Agent: "agent-3"},
		},
		caller: caller,
	}

	response := httptest.NewRecorder()
	s.handler().ServeHTTP(response, httptest.NewRequest("POST", "/api/scale", strings.NewReader(`{"slaves":1}`)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("scale down status = %d, want %d", response.Code, http.StatusAccepted)
	}
	callsSeen := caller.snapshot()
	if len(callsSeen) != 2 {
		t.Fatalf("Mesos calls = %d, want two KILL calls", len(callsSeen))
	}
	for i, want := range []string{"slave-3-task", "slave-2-task"} {
		if callsSeen[i].GetType() != scheduler.Call_KILL {
			t.Fatalf("call %d type = %s, want KILL", i, callsSeen[i].GetType())
		}
		if got := callsSeen[i].GetKill().GetTaskID().Value; got != want {
			t.Fatalf("call %d task ID = %q, want %q", i, got, want)
		}
		if got := callsSeen[i].GetKill().GetAgentID().GetValue(); got != "agent-"+string(rune('0'+3-i)) {
			t.Fatalf("call %d agent ID = %q", i, got)
		}
		if got := callsSeen[i].GetFrameworkID().GetValue(); got != "framework-test" {
			t.Fatalf("call %d framework ID = %q", i, got)
		}
	}
	if len(s.tasks) != 1 || s.tasks["slave-1-task"] == nil || s.cfg.Slaves != 1 {
		t.Fatalf("state after scale down = tasks=%v slaves=%d", s.tasks, s.cfg.Slaves)
	}
}

func TestScaleUpDoesNotKillTasks(t *testing.T) {
	caller := &recordingCaller{}
	s := &Scheduler{cfg: Config{Name: "test", Slaves: 1}, tasks: map[string]*Task{
		"slave-1-task": {ID: "slave-1-task", Role: "slave-1", State: "TASK_RUNNING", Agent: "agent-1"},
	}, caller: caller}
	response := httptest.NewRecorder()
	s.handler().ServeHTTP(response, httptest.NewRequest("POST", "/api/scale", strings.NewReader(`{"slaves":2}`)))
	if response.Code != http.StatusAccepted || len(caller.snapshot()) != 0 {
		t.Fatalf("scale up status=%d calls=%d, want accepted and no Mesos calls", response.Code, len(caller.snapshot()))
	}
}

func TestScaleDownKillsSurplusMasters(t *testing.T) {
	caller := &recordingCaller{}
	s := &Scheduler{cfg: Config{Name: "test", Masters: 3}, frameworkID: "framework-test", tasks: map[string]*Task{
		"m1": {ID: "m1", Role: "master-1", State: "TASK_RUNNING", Agent: "agent-1"},
		"m2": {ID: "m2", Role: "master-2", State: "TASK_RUNNING", Agent: "agent-2"},
		"m3": {ID: "m3", Role: "master-3", State: "TASK_RUNNING", Agent: "agent-3"},
	}, caller: caller}
	if err := s.scaleMasters(context.Background(), 1); err != nil {
		t.Fatalf("scale masters: %v", err)
	}
	callsSeen := caller.snapshot()
	if len(callsSeen) != 2 || callsSeen[0].GetKill().GetTaskID().Value != "m3" || callsSeen[1].GetKill().GetTaskID().Value != "m2" {
		t.Fatalf("kill calls = %#v, want m3 then m2", callsSeen)
	}
	if s.cfg.Masters != 1 || len(s.tasks) != 1 || s.tasks["m1"] == nil {
		t.Fatalf("scaled master state = masters=%d tasks=%v", s.cfg.Masters, s.tasks)
	}
}

func TestStopKillsSlavesBeforeMastersAndClearsTasks(t *testing.T) {
	caller := &recordingCaller{}
	s := &Scheduler{
		cfg:         Config{Name: "test"},
		desired:     true,
		frameworkID: "framework-test",
		framework:   &lib.FrameworkInfo{ID: &lib.FrameworkID{Value: "framework-test"}},
		tasks: map[string]*Task{
			"master-task":  {ID: "master-task", Role: "master", State: "TASK_RUNNING", Agent: "agent-master"},
			"slave-2-task": {ID: "slave-2-task", Role: "slave-2", State: "TASK_RUNNING", Agent: "agent-2"},
			"slave-1-task": {ID: "slave-1-task", Role: "slave-1", State: "TASK_RUNNING", Agent: "agent-1"},
			"finished":     {ID: "finished", Role: "slave-3", State: "TASK_FINISHED", Agent: "agent-3"},
		},
		caller: caller,
	}

	s.stop()

	callsSeen := caller.snapshot()
	if len(callsSeen) != 3 {
		t.Fatalf("Mesos calls = %d, want three KILL calls", len(callsSeen))
	}
	want := []struct{ id, agent string }{
		{"slave-1-task", "agent-1"},
		{"slave-2-task", "agent-2"},
		{"master-task", "agent-master"},
	}
	for i, expected := range want {
		call := callsSeen[i]
		if call.GetType() != scheduler.Call_KILL || call.GetKill().GetTaskID().Value != expected.id || call.GetKill().GetAgentID().GetValue() != expected.agent {
			t.Fatalf("call %d = %#v, want KILL task=%s agent=%s", i, call, expected.id, expected.agent)
		}
		if got := call.GetFrameworkID().GetValue(); got != "framework-test" {
			t.Fatalf("call %d framework ID = %q, want framework-test", i, got)
		}
	}
	if s.desired || len(s.tasks) != 0 {
		t.Fatalf("scheduler after stop = desired=%v tasks=%v, want false and no tasks", s.desired, s.tasks)
	}
	if s.frameworkID != "" || s.framework.GetID() != nil {
		t.Fatalf("scheduler framework identity after stop = (%q, %#v), want empty and nil", s.frameworkID, s.framework.GetID())
	}
}

func TestScaleDownPreservesMasterQuorum(t *testing.T) {
	caller := &recordingCaller{}
	s := &Scheduler{cfg: Config{Name: "test", Masters: 3}, frameworkID: "framework-test", tasks: map[string]*Task{
		"m1": {ID: "m1", Role: "master-1", State: "TASK_RUNNING", Agent: "agent-1"},
		"m2": {ID: "m2", Role: "master-2", State: "TASK_RUNNING", Agent: "agent-2"},
		"m3": {ID: "m3", Role: "master-3", State: "TASK_STAGING", Agent: "agent-3"},
	}, caller: caller}

	if err := s.scaleMasters(context.Background(), 2); err != nil {
		t.Fatalf("scaling to a target with quorum available: %v", err)
	}
	if s.cfg.Masters != 2 || len(caller.snapshot()) != 1 {
		t.Fatalf("scaled state = masters=%d kills=%d, want 2 masters and one kill", s.cfg.Masters, len(caller.snapshot()))
	}

	s = &Scheduler{cfg: Config{Name: "test", Masters: 3}, frameworkID: "framework-test", tasks: map[string]*Task{
		"m1": {ID: "m1", Role: "master-1", State: "TASK_RUNNING", Agent: "agent-1"},
		"m2": {ID: "m2", Role: "master-2", State: "TASK_STAGING", Agent: "agent-2"},
		"m3": {ID: "m3", Role: "master-3", State: "TASK_STAGING", Agent: "agent-3"},
	}, caller: &recordingCaller{}}
	if err := s.scaleMasters(context.Background(), 2); err == nil {
		t.Fatal("scaling must fail when the target would have fewer than two running masters")
	}
	if s.cfg.Masters != 3 || len(s.tasks) != 3 {
		t.Fatalf("failed scale changed state: masters=%d tasks=%d", s.cfg.Masters, len(s.tasks))
	}
}

func TestMasterQuorum(t *testing.T) {
	for masters, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 2, 4: 3, 5: 3} {
		if got := masterQuorum(masters); got != want {
			t.Fatalf("masterQuorum(%d) = %d, want %d", masters, got, want)
		}
	}
}

func TestScaleUpFromLegacyMasterSchedulesSecondMaster(t *testing.T) {
	s := &Scheduler{cfg: Config{Masters: 2, Slaves: 1}, tasks: map[string]*Task{
		"legacy": {ID: "legacy", Role: "master", State: "TASK_RUNNING"},
	}}
	if got := s.nextRole(); got != "master-2" {
		t.Fatalf("next role after scaling legacy master = %q, want master-2", got)
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

	// Test TASK_STAGING state - should wait instead of launching duplicate masters
	s.tasks["master-1"] = &Task{ID: "master-1", Role: "master", State: "TASK_STAGING"}
	next = s.nextRole()
	if next != "" {
		t.Fatalf("Expected no role when master is TASK_STAGING, got %q", next)
	}

	// Test TASK_STARTING state - should wait instead of launching duplicate masters
	s.tasks["master-1"] = &Task{ID: "master-1", Role: "master", State: "TASK_STARTING"}
	next = s.nextRole()
	if next != "" {
		t.Fatalf("Expected no role when master is TASK_STARTING, got %q", next)
	}

	// Test TASK_UNKNOWN state - should wait instead of launching duplicate masters
	s.tasks["master-1"] = &Task{ID: "master-1", Role: "master", State: "TASK_UNKNOWN"}
	next = s.nextRole()
	if next != "" {
		t.Fatalf("Expected no role when master is TASK_UNKNOWN, got %q", next)
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

func TestStatusIncludesWarningsForResourceShortageAndFailedRole(t *testing.T) {
	s := &Scheduler{frameworkID: "framework-1", desired: true, cfg: Config{Slaves: 1}, tasks: map[string]*Task{
		"master-failed": {ID: "master-failed", Role: "master", State: "TASK_FAILED"},
	}}
	s.resourceShortage = true
	recording := httptest.NewRecorder()
	s.handler().ServeHTTP(recording, httptest.NewRequest("GET", "/api/status", nil))
	var got struct {
		Warnings []string `json:"warnings"`
	}
	if err := json.Unmarshal(recording.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(got.Warnings) != 2 || !strings.Contains(strings.Join(got.Warnings, " "), "resources") || !strings.Contains(strings.Join(got.Warnings, " "), "master") {
		t.Fatalf("warnings = %#v, want resource and failed master warnings", got.Warnings)
	}
}

func TestBuildTaskInfoAddsConfiguredDockerVolume(t *testing.T) {
	s := NewScheduler(Config{Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, ValkeyVolumeDriver: "convoy", ValkeyVolumeName: "valkey-data", ValkeyVolumePath: "/var/lib/valkey"})
	task := s.buildTaskInfo("master", "task-1", lib.Offer{AgentID: lib.AgentID{Value: "agent-1"}})
	if len(task.Container.GetVolumes()) != 1 {
		t.Fatalf("volumes = %d, want 1", len(task.Container.GetVolumes()))
	}
	volume := task.Container.GetVolumes()[0]
	if volume.GetContainerPath() != "/var/lib/valkey" || volume.GetMode() != lib.Volume_RW || volume.GetSource().GetType() != lib.Volume_Source_DOCKER_VOLUME {
		t.Fatalf("volume = %#v, want RW Docker volume at /var/lib/valkey", volume)
	}
	if volume.GetSource().GetDockerVolume().GetDriver() != "convoy" || volume.GetSource().GetDockerVolume().GetName() != "valkey-data" {
		t.Fatalf("docker volume = %#v, want convoy/valkey-data", volume.GetSource().GetDockerVolume())
	}
}

func TestBuildTaskInfoOmitsDockerVolumeWhenNameUnset(t *testing.T) {
	s := NewScheduler(Config{Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, ValkeyVolumeDriver: "convoy"})
	task := s.buildTaskInfo("master", "task-1", lib.Offer{AgentID: lib.AgentID{Value: "agent-1"}})
	if len(task.Container.GetVolumes()) != 0 {
		t.Fatalf("volumes = %#v, want none when volume name is unset", task.Container.GetVolumes())
	}
}

func TestLoadConfigReadsValkeyVolumeSettings(t *testing.T) {
	t.Setenv("VALKEY_VOLUME_DRIVER", "local")
	t.Setenv("VALKEY_VOLUME_NAME", "persistent-valkey")
	t.Setenv("VALKEY_VOLUME_PATH", "/data")
	c := loadConfig()
	if c.ValkeyVolumeDriver != "local" || c.ValkeyVolumeName != "persistent-valkey" || c.ValkeyVolumePath != "/data" {
		t.Fatalf("volume config = (%q, %q, %q), want (local, persistent-valkey, /data)", c.ValkeyVolumeDriver, c.ValkeyVolumeName, c.ValkeyVolumePath)
	}
}

func TestBuildTaskInfoUsesSeparateValkeyAuthentication(t *testing.T) {
	s := NewScheduler(Config{Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, MasterHost: "master.mesos", ValkeyPassword: "client-secret", ValkeyReplicationPassword: "replica-secret"})
	master := s.buildTaskInfo("master", "master-1", lib.Offer{})
	slave := s.buildTaskInfo("slave-1", "slave-1-1", lib.Offer{})
	if master.Command.GetValue() != "valkey-server --port 6379 --requirepass client-secret" {
		t.Fatalf("master command = %q", master.Command.GetValue())
	}
	wantSlave := "valkey-server --port 6379 --requirepass client-secret --replicaof master.mesos 6379 --masterauth replica-secret"
	if slave.Command.GetValue() != wantSlave {
		t.Fatalf("slave command = %q, want %q", slave.Command.GetValue(), wantSlave)
	}
}

func TestBuildTaskInfoOmitsValkeyAuthenticationWhenUnset(t *testing.T) {
	s := NewScheduler(Config{Image: "valkey:test", CPU: .2, Memory: 128, Port: 6379, MasterHost: "master.mesos"})
	task := s.buildTaskInfo("slave-1", "slave-1-1", lib.Offer{})
	if strings.Contains(task.Command.GetValue(), "requirepass") || strings.Contains(task.Command.GetValue(), "masterauth") {
		t.Fatalf("unauthenticated command = %q", task.Command.GetValue())
	}
}

func TestLoadConfigReadsValkeyAuthentication(t *testing.T) {
	t.Setenv("VALKEY_PASSWORD", "client-secret")
	t.Setenv("VALKEY_REPLICATION_PASSWORD", "replica-secret")
	c := loadConfig()
	if c.ValkeyPassword != "client-secret" || c.ValkeyReplicationPassword != "replica-secret" {
		t.Fatalf("Valkey auth config = (%q, %q)", c.ValkeyPassword, c.ValkeyReplicationPassword)
	}
}
