package main

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/extras/scheduler/controller"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler/calls"
	logrus "github.com/sirupsen/logrus"
)

func scalar(name string, v float64) lib.Resource {
	return lib.Resource{Name: name, Type: lib.SCALAR, Scalar: &lib.Value_Scalar{Value: v}}
}

func offerAgentURL(o lib.Offer) string {
	if o.URL == nil || strings.TrimSpace(o.URL.Scheme) == "" {
		return ""
	}
	host := o.URL.Address.GetHostname()
	if host == "" {
		host = o.URL.Address.GetIP()
	}
	port := o.URL.Address.GetPort()
	if host == "" || port <= 0 {
		return ""
	}
	return fmt.Sprintf("%s://%s", o.URL.Scheme, net.JoinHostPort(host, strconv.Itoa(int(port))))
}

func (s *Scheduler) buildTaskInfo(role, id string, o lib.Offer) lib.TaskInfo {
	cmd := fmt.Sprintf("valkey-server --port %d", s.cfg.Port)
	if s.cfg.ValkeyPassword != "" {
		cmd += " --requirepass " + s.cfg.ValkeyPassword
	}
	if !isMasterRole(role) {
		cmd += fmt.Sprintf(" --replicaof %s %d", s.cfg.MasterHost, s.cfg.Port)
		if s.cfg.ValkeyReplicationPassword != "" {
			cmd += " --masterauth " + s.cfg.ValkeyReplicationPassword
		}
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
	container := &lib.ContainerInfo{Type: &typ, Hostname: &hostname, Docker: &lib.ContainerInfo_DockerInfo{Image: image, Network: &dockerNetwork}, NetworkInfos: networkInfos}
	if s.cfg.ValkeyVolumeName != "" {
		driver := s.cfg.ValkeyVolumeDriver
		container.Volumes = []lib.Volume{{Mode: func() *lib.Volume_Mode { mode := lib.Volume_RW; return &mode }(), ContainerPath: s.cfg.ValkeyVolumePath, Source: &lib.Volume_Source{Type: lib.Volume_Source_DOCKER_VOLUME, DockerVolume: &lib.Volume_Source_DockerVolume{Driver: &driver, Name: s.cfg.ValkeyVolumeName}}}}
	}
	resources := []lib.Resource{scalar("cpus", s.cfg.CPU), scalar("mem", s.cfg.Memory)}
	if s.cfg.Disk > 0 {
		resources = append(resources, scalar("disk", s.cfg.Disk))
	}
	return lib.TaskInfo{Name: taskName, TaskID: lib.TaskID{Value: id}, AgentID: o.AgentID, Resources: resources, Command: &lib.CommandInfo{Shell: &shell, Value: &cmd}, Container: container}
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
	actualDisk := 0.0

	for _, resource := range o.Resources {
		switch resource.GetName() {
		case "cpus":
			actualCPU += resource.GetScalar().GetValue()
		case "mem":
			actualMem += resource.GetScalar().GetValue()
		case "disk":
			actualDisk += resource.GetScalar().GetValue()
		}
	}

	// Check if offer has sufficient CPU and Memory
	if actualCPU < requiredCPU || actualMem < requiredMem || actualDisk < s.cfg.Disk {
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
		s.setSchedulerConnected(false)
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
		s.setSchedulerConnected(true)
		changed := s.recordFrameworkID(frameworkID)
		if changed {
			s.mu.Lock()
			if s.framework != nil {
				s.framework.ID = &lib.FrameworkID{Value: frameworkID}
			}
			s.mu.Unlock()
			s.save()
		}
		// Redis may contain task records from while the framework was
		// disconnected. Reconcile immediately after subscribing so Mesos can
		// report records that disappeared instead of waiting for the periodic
		// reconciliation interval.
		if s.hasTrackedTasks() {
			if err := s.reconcileTrackedTasks(ctx); err != nil {
				logrus.WithError(err).Warn("initial scheduler reconcile failed")
			}
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
				agentURL := offerAgentURL(o)
				s.tasks[id] = &Task{ID: id, Role: role, State: "TASK_STAGING", Agent: o.AgentID.Value, Host: o.Hostname, AgentURL: agentURL, Port: s.cfg.Port, CPU: s.cfg.CPU, Memory: s.cfg.Memory, Disk: s.cfg.Disk, Updated: time.Now()}
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

			if isTerminalTaskState(st.State.String()) {
				delete(s.tasks, st.TaskID.Value)
			}
		}
		s.mu.Unlock()
		s.save()
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

func (s *Scheduler) hasTrackedTasks() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.tasks {
		if !isTerminalTaskState(task.State) {
			return true
		}
	}
	return false
}

func (s *Scheduler) reconcileTrackedTasks(ctx context.Context) error {
	s.mu.Lock()
	frameworkID := s.frameworkID
	tasks := make(map[string]string, len(s.tasks))
	for id, task := range s.tasks {
		if !isTerminalTaskState(task.State) {
			tasks[id] = task.Agent
		}
	}
	s.mu.Unlock()
	if frameworkID == "" || len(tasks) == 0 || s.caller == nil {
		return nil
	}
	call := calls.Reconcile(calls.ReconcileTasks(tasks)).With(calls.Framework(frameworkID))
	return calls.CallNoData(ctx, s.caller, call)
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
	var cancel context.CancelFunc
	if !s.cfg.DryRun {
		ctx, cancel = context.WithCancel(context.Background())
		s.runCancel = cancel
		s.runDone = make(chan struct{})
		s.running = true
	}
	s.mu.Unlock()
	s.save()
	if !s.cfg.DryRun {
		go func() {
			defer func() {
				cancel()
				s.mu.Lock()
				s.running = false
				s.runCancel = nil
				close(s.runDone)
				s.runDone = nil
				s.mu.Unlock()
			}()
			if s.cfg.ReconcileLoopTime > 0 {
				go s.reconcileLoop(ctx)
			}
			if e := controller.Run(ctx, s.framework, s.caller, controller.WithRegistrationTokens(schedulerRegistrationTokens(ctx)), controller.WithEventHandler(eventHandler{s}), controller.WithFrameworkID(func() string {
				return s.currentFrameworkID()
			}), controller.WithSubscriptionTerminated(func(e error) {
				s.setSchedulerConnected(false)
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
	frameworkID := s.frameworkID
	type taskToKill struct{ id, role, agent string }
	toKill := make([]taskToKill, 0, len(s.tasks))
	for _, task := range s.tasks {
		if !isTerminalTaskState(task.State) {
			toKill = append(toKill, taskToKill{id: task.ID, role: task.Role, agent: task.Agent})
		}
	}
	s.mu.Unlock()

	sort.Slice(toKill, func(i, j int) bool {
		iMaster, jMaster := isMasterRole(toKill[i].role), isMasterRole(toKill[j].role)
		if iMaster != jMaster {
			return !iMaster
		}
		if toKill[i].role != toKill[j].role {
			return toKill[i].role < toKill[j].role
		}
		return toKill[i].id < toKill[j].id
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, task := range toKill {
		call := calls.Kill(task.id, task.agent).With(calls.Framework(frameworkID))
		if err := calls.CallNoData(ctx, s.caller, call); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{"task_id": task.id, "agent_id": task.agent}).Warn("failed to kill task during scheduler stop")
		}
	}

	s.mu.Lock()
	s.tasks = make(map[string]*Task)
	// Clear both copies of the ID. Clearing only Scheduler.frameworkID leaves
	// calls.Subscribe with the stale FrameworkInfo.ID, which Mesos rejects as a
	// mismatch on the next start.
	s.frameworkID = ""
	if s.framework != nil {
		s.framework.ID = nil
	}
	done := s.runDone
	if s.runCancel != nil {
		s.runCancel()
		s.runCancel = nil
	}
	s.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			logrus.Warn("scheduler stop timed out waiting for subscription shutdown")
		}
	}
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

// masterQuorum is the majority of the configured masters. A deployment with
// one master has no majority requirement beyond that single master.
func masterQuorum(masters int) int {
	if masters <= 1 {
		return 1
	}
	return masters/2 + 1
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
		running   bool
	}
	surplus := make([]surplusTask, 0)
	runningMasters := 0
	for _, task := range s.tasks {
		index := masterIndex(task.Role)
		if isMasterRole(task.Role) && task.State == "TASK_RUNNING" {
			runningMasters++
		}
		if index > target && index > 0 && !isTerminalTaskState(task.State) {
			surplus = append(surplus, surplusTask{id: task.ID, agent: task.Agent, index: index, running: task.State == "TASK_RUNNING"})
		}
	}
	if target > 1 && target < s.desiredMasterCount() {
		for _, task := range surplus {
			if task.running {
				runningMasters--
			}
		}
		if runningMasters < masterQuorum(target) {
			s.mu.Unlock()
			return fmt.Errorf("master quorum unavailable: need %d running masters, have %d after scaling to %d", masterQuorum(target), runningMasters, target)
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

// reconcileLoop periodically asks Mesos for status updates for tracked tasks.
func (s *Scheduler) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ReconcileLoopTime)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.reconcileTrackedTasks(ctx); err != nil && ctx.Err() == nil {
				logrus.WithError(err).Warn("scheduler reconcile failed")
			}
		case <-ctx.Done():
			return
		}
	}
}
