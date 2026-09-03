package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"

	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/encoding/codecs"
	"github.com/m3scluster/clusterd-go/api/v1/lib/extras/scheduler/controller"
	httpcli "github.com/m3scluster/clusterd-go/api/v1/lib/httpcli"
	httpsched "github.com/m3scluster/clusterd-go/api/v1/lib/httpcli/httpsched"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler/calls"
	"github.com/redis/go-redis/v9"
	logrus "github.com/sirupsen/logrus"
	"valkey-mesos-framework/utils"
)

type Config struct {
	Master, Image, Role, Name, User, RedisServer, ValkeyMetricsAddr, CNI, Domain, MasterHost, Listen string
	Password, RedisPassword                                                                          string
	RedisDB, Masters, Slaves                                                                         int
	CPU, Memory                                                                                      float64
	Port                                                                                             int
	DryRun, InsecureTLS                                                                              bool
}

// This model supports one master and requires at least one replica. Keep the
// invariant in the backend so callers cannot weaken it by bypassing the UI.
const minSlaves = 1
const minMasters = 1

type Task struct {
	ID, Role, State, Agent, Host string
	Port                         int
	Updated                      time.Time
}

type valkeyMetricsReader interface {
	Info(context.Context, ...string) (string, error)
}

type redisInfoReader struct{ client *redis.Client }

func (r redisInfoReader) Info(ctx context.Context, sections ...string) (string, error) {
	return r.client.Info(ctx, sections...).Result()
}

type Scheduler struct {
	cfg              Config
	mu               sync.Mutex
	frameworkID      string
	desired          bool
	tasks            map[string]*Task
	state            *redis.Client
	caller           calls.Caller
	framework        *lib.FrameworkInfo
	runCancel        context.CancelFunc
	running          bool
	resourceShortage bool
	metricsReader    valkeyMetricsReader
}

const schedulerReconnectBackoff = time.Second

// schedulerRegistrationTokens rate-limits controller re-subscriptions. The
// Mesos event stream can end with io.EOF when the master closes a connection;
// without a token gate controller.Run immediately reconnects in a tight loop.
func schedulerRegistrationTokens(ctx context.Context) <-chan struct{} {
	tokens := make(chan struct{}, 1)
	tokens <- struct{}{}
	go func() {
		ticker := time.NewTicker(schedulerReconnectBackoff)
		defer ticker.Stop()
		defer close(tokens)
		for {
			select {
			case <-ticker.C:
				select {
				case tokens <- struct{}{}:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return tokens
}

func atoi(k string, d int) int {
	v, e := strconv.Atoi(utils.Getenv(k, strconv.Itoa(d)))
	if e != nil {
		return d
	}
	return v
}
func loadConfig() Config {
	master := utils.Getenv("MESOS_MASTER", "127.0.0.1:5050")
	if !strings.Contains(master, "://") {
		scheme := "http"
		if utils.Getenv("MESOS_SSL", "false") == "true" {
			scheme = "https"
		}
		master = scheme + "://" + master
	}
	name := utils.Getenv("FRAMEWORK_NAME", "valkey-framework")
	cni := utils.Getenv("MESOS_CNI", "weave")
	if value, ok := utils.LookupEnv("MESOS_CNI"); ok {
		cni = value
	}
	domainDefault := "mesos"
	if cni == "weave" {
		domainDefault = "weave.local"
	}
	domain := strings.Trim(utils.Getenv("MESOS_DOMAIN", domainDefault), ".")
	// Mesos-DNS uses the task hostname and domain; the framework name is not part
	// of the DNS name (for example, master.weave.local).
	masterHost := "master"
	if domain != "" {
		masterHost += "." + domain
	}
	slaves := atoi("VALKEY_SLAVES", 2)
	if slaves < minSlaves {
		slaves = minSlaves
	}
	masters := atoi("VALKEY_MASTERS", 1)
	if masters < minMasters {
		masters = minMasters
	}
	port := atoi("VALKEY_PORT", 6379)
	metricsAddr := utils.Getenv("VALKEY_METRICS_ADDR", net.JoinHostPort(masterHost, strconv.Itoa(port)))
	return Config{Master: master, Image: utils.Getenv("VALKEY_IMAGE", "valkey/valkey:8-alpine"), Role: utils.Getenv("MESOS_ROLE", "*"), Name: name, User: utils.Getenv("FRAMEWORK_USER", utils.Getenv("USER", "root")), Password: utils.Getenv("MESOS_PASSWORD", ""), RedisServer: utils.Getenv("REDIS_SERVER", "redis.weave.local:6379"), ValkeyMetricsAddr: metricsAddr, RedisPassword: utils.Getenv("REDIS_PASSWORD", ""), RedisDB: atoi("REDIS_DB", 10), CNI: cni, Domain: domain, MasterHost: utils.Getenv("VALKEY_MASTER_HOST", masterHost), Masters: masters, Slaves: slaves, CPU: floatEnv("VALKEY_CPU", .2), Memory: floatEnv("VALKEY_MEMORY_MB", 256), Port: port, Listen: utils.Getenv("LISTEN_ADDR", "0.0.0.0:10001"), DryRun: utils.Getenv("MESOS_DRY_RUN", "false") == "true", InsecureTLS: utils.Getenv("MESOS_TLS_INSECURE", "false") == "true"}
}
func floatEnv(k string, d float64) float64 {
	v, e := strconv.ParseFloat(utils.Getenv(k, strconv.FormatFloat(d, 'f', -1, 64)), 64)
	if e != nil {
		return d
	}
	return v
}

func NewScheduler(c Config) *Scheduler {
	endpoint := strings.TrimRight(c.Master, "/") + "/api/v1/scheduler"
	opts := []httpcli.Opt{
		httpcli.Endpoint(endpoint),
		httpcli.Codec(codecs.ByMediaType[codecs.MediaTypeJSON]),
	}
	configOpts := []httpcli.ConfigOpt{httpcli.Timeout(15 * time.Second)}
	if c.InsecureTLS {
		configOpts = append(configOpts, httpcli.TLSConfig(&tls.Config{InsecureSkipVerify: true}))
	}
	if u := utils.Getenv("MESOS_USERNAME", ""); u != "" && c.Password != "" {
		configOpts = append(configOpts, httpcli.BasicAuth(u, c.Password))
	}
	opts = append(opts, httpcli.Do(httpcli.With(configOpts...)))
	cli := httpcli.New(opts...)
	ft := float64(3600)
	role := c.Role
	s := &Scheduler{cfg: c, tasks: map[string]*Task{}, state: redis.NewClient(&redis.Options{Addr: c.RedisServer, Password: c.RedisPassword, DB: c.RedisDB}), caller: httpsched.NewCaller(cli), framework: &lib.FrameworkInfo{User: c.User, Name: c.Name, Role: &role, FailoverTimeout: &ft}}
	s.metricsReader = redisInfoReader{client: redis.NewClient(&redis.Options{Addr: c.ValkeyMetricsAddr})}
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
	if s.state != nil {
		_ = s.state.Set(context.Background(), s.cfg.Name+":state", b, 0).Err()
	}
}
func (s *Scheduler) load() {
	// Redis is the only durable source of truth.
	if s.state == nil {
		return
	}
	if b, err := s.state.Get(context.Background(), s.cfg.Name+":state").Result(); err == nil {
		s.restoreState([]byte(b))
	}
}

func (s *Scheduler) restoreState(b []byte) bool {
	var v struct {
		FrameworkID string           `json:"framework_id"`
		Desired     bool             `json:"desired"`
		Tasks       map[string]*Task `json:"tasks"`
	}
	if json.Unmarshal(b, &v) != nil {
		return false
	}
	s.frameworkID = v.FrameworkID
	if s.framework != nil {
		if v.FrameworkID == "" {
			s.framework.ID = nil
		} else {
			s.framework.ID = &lib.FrameworkID{Value: v.FrameworkID}
		}
	}
	s.desired = v.Desired
	if v.Tasks != nil {
		s.tasks = v.Tasks
	}
	return true
}
func scalar(name string, v float64) lib.Resource {
	return lib.Resource{Name: name, Type: lib.SCALAR, Scalar: &lib.Value_Scalar{Value: v}}
}
func (s *Scheduler) buildTaskInfo(role, id string, o lib.Offer) lib.TaskInfo {
	cmd := fmt.Sprintf("valkey-server --port %d", s.cfg.Port)
	if !isMasterRole(role) {
		cmd += fmt.Sprintf(" --replicaof %s %d", s.cfg.MasterHost, s.cfg.Port)
	}
	shell := true
	image := s.cfg.Image
	typ := lib.ContainerInfo_DOCKER
	dockerNetwork := lib.ContainerInfo_DockerInfo_USER
	network := s.cfg.CNI
	networkInfos := []lib.NetworkInfo{}
	if s.cfg.CNI != "" {
		networkInfos = append(networkInfos, lib.NetworkInfo{Name: &network})
	} else {
		dockerNetwork = lib.ContainerInfo_DockerInfo_HOST
	}

	taskName := id
	if isMasterRole(role) {
		taskName = role
	}
	hostname := taskName
	if s.cfg.Domain != "" {
		hostname += "." + strings.Trim(s.cfg.Domain, ".")
	}
	return lib.TaskInfo{Name: taskName, TaskID: lib.TaskID{Value: id}, AgentID: o.AgentID, Resources: []lib.Resource{scalar("cpus", s.cfg.CPU), scalar("mem", s.cfg.Memory)}, Command: &lib.CommandInfo{Shell: &shell, Value: &cmd}, Container: &lib.ContainerInfo{Type: &typ, Hostname: &hostname, Docker: &lib.ContainerInfo_DockerInfo{Image: image, Network: &dockerNetwork}, NetworkInfos: networkInfos}}
}
func isMasterRole(role string) bool {
	return role == "master" || strings.HasPrefix(role, "master-")
}
func (s *Scheduler) desiredMasterCount() int {
	if s.cfg.Masters < minMasters {
		return minMasters
	}
	return s.cfg.Masters
}
func masterRole(index, total int) string {
	if total == 1 {
		return "master"
	}
	return fmt.Sprintf("master-%d", index)
}

func masterSlotRole(index, total int) string {
	if total == 1 && index == 1 {
		return "master"
	}
	return fmt.Sprintf("master-%d", index)
}

func (s *Scheduler) nextRole() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A master occupies the singleton slot as soon as it is launched. Waiting
	// for TASK_RUNNING here would launch one new master for every offer while
	// the first master is still staging, creating a large history of duplicate
	// tasks and potentially leaving replicas behind after the master fails.
	masterRunning := 0
	masterPending := false
	for _, t := range s.tasks {
		if !isMasterRole(t.Role) {
			continue
		}
		switch t.State {
		case "TASK_RUNNING":
			masterRunning++
		case "TASK_STAGING", "TASK_STARTING", "TASK_UNKNOWN":
			masterPending = true
		}
	}

	// If no master exists, return "master" to create one. If one is already
	// pending, decline this offer and wait for its status update instead.
	masters := s.desiredMasterCount()
	if masterRunning == 0 && !masterPending {
		return masterRole(1, masters)
	}
	if masterRunning < masters || masterPending {
		for i := 1; i <= masters; i++ {
			role := masterSlotRole(i, masters)
			found := false
			for _, t := range s.tasks {
				// "master" is the legacy name for slot 1. Accept it when
				// scaling an existing one-master deployment to multiple masters.
				isSlot := t.Role == role || (i == 1 && t.Role == "master")
				if isSlot && !isTerminalTaskState(t.State) {
					found = true
					break
				}
			}
			if !found && !masterPending {
				return role
			}
		}
		return ""
	}

	// If master is running, check for slave roles - only create slaves if any are missing or inactive
	for i := 1; i <= s.cfg.Slaves; i++ {
		r := fmt.Sprintf("slave-%d", i)
		found := false
		for _, t := range s.tasks {
			if t.Role == r && t.State != "TASK_FAILED" && t.State != "TASK_LOST" && t.State != "TASK_FINISHED" && t.State != "TASK_KILLED" && t.State != "TASK_ERROR" {
				found = true
				break
			}
		}
		if !found {
			return r
		}
	}

	// All slaves are found or already running
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

// warningsLocked reports conditions that currently prevent the desired
// topology from being available. It must be called while s.mu is held.
func (s *Scheduler) warningsLocked() []string {
	if !s.desired {
		return nil
	}
	warnings := make([]string, 0, 1)
	if s.resourceShortage {
		warnings = append(warnings, fmt.Sprintf("Mesos offers do not provide enough resources (need %.2f CPU and %.0f MB memory)", s.cfg.CPU, s.cfg.Memory))
	}
	for i := 0; i <= s.cfg.Slaves; i++ {
		role := masterRole(1, s.desiredMasterCount())
		if i > 0 {
			role = fmt.Sprintf("slave-%d", i)
		}
		failed, live := false, false
		for _, task := range s.tasks {
			if task.Role != role && !(i == 0 && isMasterRole(task.Role)) {
				continue
			}
			switch task.State {
			case "TASK_FAILED", "TASK_LOST", "TASK_ERROR", "TASK_KILLED":
				failed = true
			case "TASK_RUNNING", "TASK_STAGING", "TASK_STARTING", "TASK_UNKNOWN":
				live = true
			}
		}
		if failed && !live {
			warnings = append(warnings, fmt.Sprintf("Unable to start %s: the last task failed or was lost", role))
		}
	}
	return warnings
}

func (s *Scheduler) handleEvent(ctx context.Context, e *scheduler.Event) error {
	switch e.GetType() {
	case scheduler.Event_ERROR:
		// Mesos can invalidate a persisted framework ID. Clear it before the
		// controller's reconnect so the next SUBSCRIBE registers a new framework.
		if s.recordFrameworkID("") {
			s.mu.Lock()
			s.framework.ID = nil
			s.mu.Unlock()
			if s.state != nil {
				s.save()
			}
		}
	case scheduler.Event_SUBSCRIBED:
		frameworkID := e.GetSubscribed().FrameworkID.GetValue()
		if frameworkID == "" {
			return fmt.Errorf("mesos sent an empty framework ID")
		}
		changed := s.recordFrameworkID(frameworkID)
		if changed {
			s.mu.Lock()
			if s.framework != nil {
				s.framework.ID = &lib.FrameworkID{Value: frameworkID}
			}
			s.mu.Unlock()
			s.save()
		}
	case scheduler.Event_OFFERS:
		callCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, o := range e.GetOffers().GetOffers() {
			role := s.nextRole()
			if role == "" || !s.desired {
				if err := calls.CallNoData(callCtx, s.caller, calls.Decline(o.ID).With(calls.Framework(s.currentFrameworkID())).With(calls.RefuseSeconds(180*time.Second))); err != nil {
					logrus.WithError(err).WithField("offer_id", o.ID.Value).Error("mesos offer decline failed")
				}
				continue
			}

			// Check if offer has sufficient CPU and Memory resources
			if !s.checkOfferResources(o) {
				s.mu.Lock()
				s.resourceShortage = true
				s.mu.Unlock()
				if err := calls.CallNoData(callCtx, s.caller, calls.Decline(o.ID).With(calls.Framework(s.currentFrameworkID())).With(calls.RefuseSeconds(5*time.Second))); err != nil {
					logrus.WithError(err).WithField("offer_id", o.ID.Value).Error("mesos offer resource decline failed")
				}
				continue
			}

			id := fmt.Sprintf("%s-%d", role, time.Now().UnixNano())
			ti := s.buildTaskInfo(role, id, o)
			err := calls.CallNoData(callCtx, s.caller, calls.Accept(calls.OfferOperations{calls.OpLaunch(ti)}.WithOffers(o.ID)).With(calls.Framework(s.currentFrameworkID())).With(calls.RefuseSeconds(5*time.Second)))
			if err == nil {
				s.mu.Lock()
				s.resourceShortage = false
				s.tasks[id] = &Task{ID: id, Role: role, State: "TASK_STAGING", Agent: o.AgentID.Value, Host: o.Hostname, Port: s.cfg.Port, Updated: time.Now()}
				s.mu.Unlock()
				s.save()
			}
		}
	case scheduler.Event_UPDATE:
		callCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		st := e.GetUpdate().GetStatus()
		s.mu.Lock()
		if t := s.tasks[st.TaskID.Value]; t != nil {
			t.State = st.State.String()
			t.Updated = time.Now()

			if isTerminalTaskState(st.State.String()) {
				delete(s.tasks, st.TaskID.Value)
			}
		}
		s.mu.Unlock()
		s.save()
		if len(st.UUID) > 0 {
			ack := calls.Acknowledge(st.AgentID.Value, st.TaskID.Value, st.UUID).With(calls.Framework(s.currentFrameworkID()))
			if err := calls.CallNoData(callCtx, s.caller, ack); err != nil {
				logrus.WithError(err).WithField("task_id", st.TaskID.Value).Error("mesos status acknowledgement failed")
			}
		}
	}
	return nil
}

func isTerminalTaskState(state string) bool {
	switch state {
	case "TASK_FAILED", "TASK_LOST", "TASK_KILLED", "TASK_ERROR", "TASK_FINISHED":
		return true
	default:
		return false
	}
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
			if e := controller.Run(ctx, s.framework, s.caller, controller.WithRegistrationTokens(schedulerRegistrationTokens(ctx)), controller.WithEventHandler(eventHandler{s}), controller.WithFrameworkID(func() string {
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

// scaleSlaves reconciles running slave tasks before committing a smaller
// target. Mesos does not remove already-launched tasks when the target changes.
func (s *Scheduler) scaleSlaves(ctx context.Context, target int) error {
	s.mu.Lock()
	if target < minSlaves {
		s.mu.Unlock()
		return fmt.Errorf("at least %d slave required", minSlaves)
	}
	frameworkID := s.frameworkID
	type surplusTask struct {
		id, agent string
		index     int
	}
	surplus := make([]surplusTask, 0)
	for _, task := range s.tasks {
		if !strings.HasPrefix(task.Role, "slave-") || isTerminalTaskState(task.State) {
			continue
		}
		index, err := strconv.Atoi(strings.TrimPrefix(task.Role, "slave-"))
		if err == nil && index > target {
			surplus = append(surplus, surplusTask{id: task.ID, agent: task.Agent, index: index})
		}
	}
	s.mu.Unlock()

	sort.Slice(surplus, func(i, j int) bool { return surplus[i].index > surplus[j].index })
	for _, task := range surplus {
		call := calls.Kill(task.id, task.agent).With(calls.Framework(frameworkID))
		if err := calls.CallNoData(ctx, s.caller, call); err != nil {
			return fmt.Errorf("kill surplus slave %s: %w", task.id, err)
		}
	}

	s.mu.Lock()
	s.cfg.Slaves = target
	for _, task := range surplus {
		delete(s.tasks, task.id)
	}
	s.mu.Unlock()
	s.save()
	return nil
}

func masterIndex(role string) int {
	if role == "master" {
		return 1
	}
	if !strings.HasPrefix(role, "master-") {
		return 0
	}
	index, err := strconv.Atoi(strings.TrimPrefix(role, "master-"))
	if err != nil {
		return 0
	}
	return index
}

func (s *Scheduler) scaleMasters(ctx context.Context, target int) error {
	s.mu.Lock()
	if target < minMasters {
		s.mu.Unlock()
		return fmt.Errorf("at least %d master required", minMasters)
	}
	frameworkID := s.frameworkID
	type surplusTask struct {
		id, agent string
		index     int
	}
	surplus := make([]surplusTask, 0)
	for _, task := range s.tasks {
		index := masterIndex(task.Role)
		if index > target && index > 0 && !isTerminalTaskState(task.State) {
			surplus = append(surplus, surplusTask{id: task.ID, agent: task.Agent, index: index})
		}
	}
	s.mu.Unlock()

	sort.Slice(surplus, func(i, j int) bool { return surplus[i].index > surplus[j].index })
	for _, task := range surplus {
		call := calls.Kill(task.id, task.agent).With(calls.Framework(frameworkID))
		if err := calls.CallNoData(ctx, s.caller, call); err != nil {
			return fmt.Errorf("kill surplus master %s: %w", task.id, err)
		}
	}

	s.mu.Lock()
	s.cfg.Masters = target
	for _, task := range surplus {
		delete(s.tasks, task.id)
	}
	s.mu.Unlock()
	s.save()
	return nil
}

type eventHandler struct{ s *Scheduler }

func (h eventHandler) HandleEvent(c context.Context, e *scheduler.Event) error {
	logrus.WithField("event_type", e.GetType().String()).WithField("event_error", e.GetError().GetMessage()).Info("mesos scheduler event received")
	return h.s.handleEvent(c, e)
}

var valkeyInfoSections = []string{"memory", "clients", "stats", "replication", "cpu", "commandstats", "latencystats"}

func parseValkeyInfo(raw string) map[string]map[string]any {
	sections := make(map[string]map[string]any, len(valkeyInfoSections))
	section := ""
	for _, line := range strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			section = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "#")))
			if sections[section] == nil {
				sections[section] = make(map[string]any)
			}
			continue
		}
		separator := strings.IndexByte(line, ':')
		if section == "" || separator <= 0 {
			continue
		}
		key, value := strings.TrimSpace(line[:separator]), strings.TrimSpace(line[separator+1:])
		if number, err := strconv.ParseFloat(value, 64); err == nil {
			sections[section][key] = number
		} else {
			sections[section][key] = value
		}
	}
	return sections
}

func (s *Scheduler) handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok\n")) })
	m.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"framework_id": s.frameworkID, "desired": s.desired, "masters": s.desiredMasterCount(), "slaves": s.cfg.Slaves, "min_masters": minMasters, "min_slaves": minSlaves, "warnings": s.warningsLocked(), "tasks": s.tasks})
	})
	m.HandleFunc("/api/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		counts := map[string]int{}
		for _, task := range s.tasks {
			counts[task.State]++
		}
		frameworkID, desired, reader := s.frameworkID, s.desired, s.metricsReader
		s.mu.Unlock()
		live := counts["TASK_RUNNING"] + counts["TASK_STAGING"] + counts["TASK_STARTING"] + counts["TASK_UNKNOWN"]
		metrics := map[string]any{
			"framework_id": frameworkID,
			"desired":      desired,
			"total":        live,
			"running":      counts["TASK_RUNNING"],
			"staging":      counts["TASK_STAGING"],
			"failed":       counts["TASK_FAILED"] + counts["TASK_LOST"],
		}
		valkey := map[string]any{"available": false, "error": "Valkey metrics reader is not configured", "sections": map[string]map[string]any{}}
		if reader != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			raw, err := reader.Info(ctx, valkeyInfoSections...)
			cancel()
			if err != nil {
				valkey["error"] = err.Error()
			} else {
				valkey["available"] = true
				valkey["error"] = ""
				valkey["sections"] = parseValkeyInfo(raw)
			}
		}
		metrics["valkey"] = valkey
		_ = json.NewEncoder(w).Encode(metrics)
	})
	m.HandleFunc("/api/start", func(w http.ResponseWriter, r *http.Request) { s.start(); w.WriteHeader(202) })
	m.HandleFunc("/api/stop", func(w http.ResponseWriter, r *http.Request) { s.stop(); w.WriteHeader(202) })
	m.HandleFunc("/api/scale", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var request struct {
			Masters *int `json:"masters"`
			Slaves  *int `json:"slaves"`
		}
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&request); err != nil {
			http.Error(w, "invalid JSON request", http.StatusBadRequest)
			return
		}
		if request.Masters == nil && request.Slaves == nil {
			http.Error(w, "masters or slaves is required", http.StatusBadRequest)
			return
		}
		// Validate the complete request before reconciling either dimension so
		// a malformed combined update cannot partially change the topology.
		if request.Masters != nil && *request.Masters < minMasters {
			http.Error(w, fmt.Sprintf("at least %d master required", minMasters), http.StatusBadRequest)
			return
		}
		if request.Slaves != nil && *request.Slaves < minSlaves {
			http.Error(w, fmt.Sprintf("at least %d slave required", minSlaves), http.StatusBadRequest)
			return
		}
		if request.Masters != nil {
			if err := s.scaleMasters(r.Context(), *request.Masters); err != nil {
				status := http.StatusInternalServerError
				http.Error(w, err.Error(), status)
				return
			}
		}
		if request.Slaves != nil {
			if err := s.scaleSlaves(r.Context(), *request.Slaves); err != nil {
				status := http.StatusInternalServerError
				http.Error(w, err.Error(), status)
				return
			}
		}
		w.WriteHeader(http.StatusAccepted)
		s.mu.Lock()
		masters, slaves := s.desiredMasterCount(), s.cfg.Slaves
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"masters": masters, "slaves": slaves, "min_masters": minMasters, "min_slaves": minSlaves})
	})
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
	if utils.Getenv("START_ON_BOOT", "true") == "true" {
		s.start()
	}
	select {}
}
