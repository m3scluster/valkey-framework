package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	lib "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/controller"
	httpcli "github.com/mesos/mesos-go/api/v1/lib/httpcli"
	httpsched "github.com/mesos/mesos-go/api/v1/lib/httpcli/httpsched"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/redis/go-redis/v9"
	logrus "github.com/sirupsen/logrus"
)

type Config struct {
	Master, Image, Role, Name, User, StateFile, RedisServer, CNI, MasterHost, Listen string
	Password, RedisPassword                                                          string
	RedisDB, Slaves                                                                  int
	CPU, Memory                                                                      float64
	Port                                                                             int
	DryRun, InsecureTLS                                                              bool
}
type Task struct {
	ID, Role, State, Agent, Host string
	Port                         int
	Updated                      time.Time
}
type Scheduler struct {
	cfg         Config
	mu          sync.Mutex
	frameworkID string
	desired     bool
	tasks       map[string]*Task
	state       *redis.Client
	caller      calls.Caller
	framework   *lib.FrameworkInfo
	runCancel   context.CancelFunc
	running     bool
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func atoi(k string, d int) int {
	v, e := strconv.Atoi(env(k, strconv.Itoa(d)))
	if e != nil {
		return d
	}
	return v
}
func loadConfig() Config {
	master := env("MESOS_MASTER", "127.0.0.1:5050")
	if !strings.Contains(master, "://") {
		scheme := "http"
		if env("MESOS_SSL", "false") == "true" {
			scheme = "https"
		}
		master = scheme + "://" + master
	}
	return Config{Master: master, Image: env("VALKEY_IMAGE", "valkey/valkey:8-alpine"), Role: env("MESOS_ROLE", "*"), Name: env("FRAMEWORK_NAME", "valkey-framework"), User: env("FRAMEWORK_USER", env("USER", "root")), Password: os.Getenv("MESOS_PASSWORD"), StateFile: env("STATE_FILE", "/tmp/valkey-mesos.json"), RedisServer: env("REDIS_SERVER", "redis.weave.local:6379"), RedisPassword: os.Getenv("REDIS_PASSWORD"), RedisDB: atoi("REDIS_DB", 10), CNI: env("MESOS_CNI", "weave"), MasterHost: env("VALKEY_MASTER_HOST", env("FRAMEWORK_NAME", "valkey-framework")+".mesos"), Slaves: atoi("VALKEY_SLAVES", 2), CPU: floatEnv("VALKEY_CPU", .2), Memory: floatEnv("VALKEY_MEMORY_MB", 256), Port: atoi("VALKEY_PORT", 6379), Listen: env("LISTEN_ADDR", "0.0.0.0:10001"), DryRun: env("MESOS_DRY_RUN", "false") == "true", InsecureTLS: env("MESOS_TLS_INSECURE", "false") == "true"}
}
func floatEnv(k string, d float64) float64 {
	v, e := strconv.ParseFloat(env(k, strconv.FormatFloat(d, 'f', -1, 64)), 64)
	if e != nil {
		return d
	}
	return v
}

func NewScheduler(c Config) *Scheduler {
	endpoint := strings.TrimRight(c.Master, "/") + "/api/v1/scheduler"
	opts := []httpcli.Opt{httpcli.Endpoint(endpoint)}
	configOpts := []httpcli.ConfigOpt{httpcli.Timeout(15 * time.Second)}
	if c.InsecureTLS {
		configOpts = append(configOpts, httpcli.TLSConfig(&tls.Config{InsecureSkipVerify: true}))
	}
	if u := os.Getenv("MESOS_USERNAME"); u != "" && c.Password != "" {
		configOpts = append(configOpts, httpcli.BasicAuth(u, c.Password))
	}
	opts = append(opts, httpcli.Do(httpcli.With(configOpts...)))
	cli := httpcli.New(opts...)
	ft := float64(3600)
	role := c.Role
	s := &Scheduler{cfg: c, tasks: map[string]*Task{}, state: redis.NewClient(&redis.Options{Addr: c.RedisServer, Password: c.RedisPassword, DB: c.RedisDB}), caller: httpsched.NewCaller(cli), framework: &lib.FrameworkInfo{User: c.User, Name: c.Name, Role: &role, FailoverTimeout: &ft}}
	s.load()
	return s
}
func (s *Scheduler) currentFrameworkID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.frameworkID
}
func (s *Scheduler) recordFrameworkID(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.frameworkID == id {
		return false
	}
	s.frameworkID = id
	return true
}
func (s *Scheduler) save() {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(map[string]any{"framework_id": s.frameworkID, "desired": s.desired, "tasks": s.tasks, "config": s.cfg})
	_ = os.WriteFile(s.cfg.StateFile, b, 0600)
	_ = s.state.Set(context.Background(), s.cfg.Name+":state", b, 0).Err()
}
func (s *Scheduler) load() {
	b, e := os.ReadFile(s.cfg.StateFile)
	if e != nil {
		return
	}
	var v struct {
		FrameworkID string           `json:"framework_id"`
		Desired     bool             `json:"desired"`
		Tasks       map[string]*Task `json:"tasks"`
	}
	if json.Unmarshal(b, &v) == nil {
		s.frameworkID = v.FrameworkID
		s.desired = v.Desired
		if v.Tasks != nil {
			s.tasks = v.Tasks
		}
	}
}
func scalar(name string, v float64) lib.Resource {
	typeValue := lib.SCALAR
	return lib.Resource{Name: name, Type: &typeValue, Scalar: &lib.Value_Scalar{Value: v}}
}
func (s *Scheduler) buildTaskInfo(role, id string, o lib.Offer) lib.TaskInfo {
	cmd := fmt.Sprintf("valkey-server --port %d", s.cfg.Port)
	if role != "master" {
		cmd += fmt.Sprintf(" --replicaof %s %d", s.cfg.MasterHost, s.cfg.Port)
	}
	shell := true
	image := s.cfg.Image
	typ := lib.ContainerInfo_DOCKER
	network := s.cfg.CNI
	return lib.TaskInfo{Name: id, TaskID: lib.TaskID{Value: id}, AgentID: o.AgentID, Resources: []lib.Resource{scalar("cpus", s.cfg.CPU), scalar("mem", s.cfg.Memory)}, Command: &lib.CommandInfo{Shell: &shell, Value: &cmd}, Container: &lib.ContainerInfo{Type: &typ, Docker: &lib.ContainerInfo_DockerInfo{Image: image}, NetworkInfos: []lib.NetworkInfo{{Name: &network}}}}
}
func (s *Scheduler) nextRole() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	master := false
	for _, t := range s.tasks {
		if t.Role == "master" && t.State == "TASK_RUNNING" {
			master = true
		}
	}
	if !master {
		return "master"
	}
	for i := 1; i <= s.cfg.Slaves; i++ {
		r := fmt.Sprintf("slave-%d", i)
		found := false
		for _, t := range s.tasks {
			if t.Role == r && t.State != "TASK_FAILED" && t.State != "TASK_LOST" {
				found = true
			}
		}
		if !found {
			return r
		}
	}
	return ""
}
func (s *Scheduler) checkOfferResources(o lib.Offer) bool {
	// Check if offer has sufficient CPU and memory for this task

	// Get required CPU and Memory from config
	requiredCPU := s.cfg.CPU
	requiredMem := s.cfg.Memory

	// Find actual CPU and Memory in the offer's resources
	actualCPU := 0.0
	actualMem := 0.0

	for _, resource := range o.Resources {
		switch resource.GetName() {
		case "cpus":
			actualCPU += resource.GetScalar().GetValue()
		case "mem":
			actualMem += resource.GetScalar().GetValue()
		}
	}

	// Check if offer has sufficient CPU and Memory
	if actualCPU < requiredCPU || actualMem < requiredMem {
		return false
	}

	return true
}

func (s *Scheduler) handleEvent(ctx context.Context, e *scheduler.Event) error {
	switch e.GetType() {
	case scheduler.Event_SUBSCRIBED:
		frameworkID := e.GetSubscribed().GetFrameworkID().GetValue()
		if frameworkID == "" {
			return fmt.Errorf("mesos sent an empty framework ID")
		}
		changed := s.recordFrameworkID(frameworkID)
		if changed {
			s.save()
		}
	case scheduler.Event_OFFERS:
		for _, o := range e.GetOffers().GetOffers() {
			role := s.nextRole()
			if role == "" || !s.desired {
				_ = calls.CallNoData(ctx, s.caller, calls.Decline(o.ID).With(calls.RefuseSeconds(180*time.Second)))
				continue
			}

			// Check if offer has sufficient CPU and Memory resources
			if !s.checkOfferResources(o) {
				_ = calls.CallNoData(ctx, s.caller, calls.Decline(o.ID).With(calls.RefuseSeconds(5*time.Second)))
				continue
			}

			id := fmt.Sprintf("%s-%d", role, time.Now().UnixNano())
			ti := s.buildTaskInfo(role, id, o)
			err := calls.CallNoData(ctx, s.caller, calls.Accept(calls.OfferOperations{calls.OpLaunch(ti)}.WithOffers(o.ID)).With(calls.RefuseSeconds(5*time.Second)))
			if err == nil {
				s.mu.Lock()
				s.tasks[id] = &Task{ID: id, Role: role, State: "TASK_STAGING", Agent: o.AgentID.Value, Host: o.Hostname, Port: s.cfg.Port, Updated: time.Now()}
				s.mu.Unlock()
				s.save()
			}
		}
	case scheduler.Event_UPDATE:
		st := e.GetUpdate().GetStatus()
		s.mu.Lock()
		if t := s.tasks[st.TaskID.Value]; t != nil {
			t.State = st.State.String()
			t.Updated = time.Now()
		}
		s.mu.Unlock()
		s.save()
		if len(st.UUID) > 0 {
			_ = calls.CallNoData(ctx, s.caller, calls.Acknowledge(st.AgentID.Value, st.TaskID.Value, st.UUID))
		}
	}
	return nil
}
func (s *Scheduler) start() {
	s.mu.Lock()
	if s.running {
		s.desired = true
		s.mu.Unlock()
		s.save()
		return
	}
	s.desired = true
	var ctx context.Context
	if !s.cfg.DryRun {
		ctx, s.runCancel = context.WithCancel(context.Background())
		s.running = true
	}
	s.mu.Unlock()
	s.save()
	if !s.cfg.DryRun {
		go func() {
			defer func() {
				s.mu.Lock()
				s.running = false
				s.runCancel = nil
				s.mu.Unlock()
			}()
			if e := controller.Run(ctx, s.framework, s.caller, controller.WithEventHandler(eventHandler{s}), controller.WithFrameworkID(func() string {
				return s.currentFrameworkID()
			}), controller.WithSubscriptionTerminated(func(e error) {
				if e != nil {
					logrus.WithError(e).Error("scheduler stopped")
				}
			})); e != nil {
				logrus.WithError(e).Error("scheduler failed")
			}
		}()
	}
}
func (s *Scheduler) stop() {
	s.mu.Lock()
	s.desired = false
	if s.runCancel != nil {
		s.runCancel()
	}
	s.mu.Unlock()
	s.save()
}

type eventHandler struct{ s *Scheduler }

func (h eventHandler) HandleEvent(c context.Context, e *scheduler.Event) error {
	return h.s.handleEvent(c, e)
}
func (s *Scheduler) handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok\n")) })
	m.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"framework_id": s.frameworkID, "desired": s.desired, "tasks": s.tasks})
	})
	m.HandleFunc("/api/start", func(w http.ResponseWriter, r *http.Request) { s.start(); w.WriteHeader(202) })
	m.HandleFunc("/api/stop", func(w http.ResponseWriter, r *http.Request) { s.stop(); w.WriteHeader(202) })
	return m
}
func main() {
	c := loadConfig()
	s := NewScheduler(c)
	go func() {
		if e := http.ListenAndServe(c.Listen, s.handler()); e != nil {
			logrus.WithError(e).Error("http server failed")
		}
	}()
	if env("START_ON_BOOT", "true") == "true" {
		s.start()
	}
	select {}
}
