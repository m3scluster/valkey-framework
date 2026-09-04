package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type mesosTaskStatistics struct {
	ExecutorID string `json:"executor_id"`
	Source     string `json:"source"`
	FrameworkID string `json:"framework_id"`
	Statistics struct {
		CPUsLimit          float64 `json:"cpus_limit"`
		CPUsUserTimeSecs   float64 `json:"cpus_user_time_secs"`
		CPUsSystemTimeSecs float64 `json:"cpus_system_time_secs"`
		MemRSSBytes        float64 `json:"mem_rss_bytes"`
		DiskUsedBytes      float64 `json:"disk_space_used_bytes"`
	} `json:"statistics"`
}

func (s *Scheduler) taskUsage(ctx context.Context, task *Task, frameworkID string) (map[string]any, error) {
	if task.AgentURL == "" || s.mesosHTTPClient == nil {
		return nil, fmt.Errorf("Mesos agent URL is unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(task.AgentURL, "/")+"/monitor/statistics", nil)
	if err != nil { return nil, err }
	response, err := s.mesosHTTPClient.Do(request)
	if err != nil { return nil, err }
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK { return nil, fmt.Errorf("Mesos agent returned HTTP %s", response.Status) }
	var statistics []mesosTaskStatistics
	if err := json.NewDecoder(response.Body).Decode(&statistics); err != nil { return nil, err }
	for _, item := range statistics {
		if item.FrameworkID != "" && frameworkID != "" && item.FrameworkID != frameworkID { continue }
		if item.Source != task.ID && item.ExecutorID != task.ID { continue }
		usage := map[string]any{"usage_available": true, "usage_cpu_cores": item.Statistics.CPUsLimit, "usage_memory_mb": item.Statistics.MemRSSBytes / (1024 * 1024)}
		if item.Statistics.DiskUsedBytes > 0 { usage["usage_disk_mb"] = item.Statistics.DiskUsedBytes / (1024 * 1024) }
		return usage, nil
	}
	return nil, fmt.Errorf("no statistics found for task %s", task.ID)
}

func (s *Scheduler) handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok\n")) })
	m.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"framework_id": s.frameworkID, "scheduler_connected": s.schedulerConnected, "desired": s.desired, "masters": s.desiredMasterCount(), "slaves": s.cfg.Slaves, "min_masters": minMasters, "min_slaves": minSlaves, "resource_limits": map[string]float64{"cpus": s.cfg.CPU, "memory_mb": s.cfg.Memory, "disk_mb": s.cfg.Disk}, "warnings": s.warningsLocked(), "tasks": s.tasks})
	})
	m.HandleFunc("/api/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		counts := map[string]int{}
		if s.schedulerConnected {
			for _, task := range s.tasks {
				counts[task.State]++
			}
		}
		frameworkID, connected, desired, reader, client := s.frameworkID, s.schedulerConnected, s.desired, s.metricsReader, s.mesosHTTPClient
		s.mu.Unlock()
		live := counts["TASK_RUNNING"] + counts["TASK_STAGING"] + counts["TASK_STARTING"] + counts["TASK_UNKNOWN"]
		nodeMetrics := make(map[string]any)
		if connected {
			s.mu.Lock()
			tasks := make([]*Task, 0, len(s.tasks))
			for id, task := range s.tasks {
				if !isTerminalTaskState(task.State) {
					tasks = append(tasks, task)
				}
			}
			s.mu.Unlock()
			for _, task := range tasks {
				metric := map[string]any{"allocated_cpu": task.CPU, "allocated_memory_mb": task.Memory, "allocated_disk_mb": task.Disk, "usage_available": false, "usage_error": "Mesos agent statistics unavailable"}
				if client != nil {
					if usage, err := s.taskUsage(r.Context(), task, frameworkID); err == nil {
						for key, value := range usage { metric[key] = value }
					} else { metric["usage_error"] = err.Error() }
				}
				nodeMetrics[task.ID] = metric
			}
		}
		metrics := map[string]any{
			"framework_id":        frameworkID,
			"scheduler_connected": connected,
			"desired":             desired,
			"total":               live,
			"running":             counts["TASK_RUNNING"],
			"staging":             counts["TASK_STAGING"],
			"failed":              counts["TASK_FAILED"] + counts["TASK_LOST"],
			"nodes":               nodeMetrics,
		}
		valkey := map[string]any{"available": false, "error": "Valkey nodes are not running", "sections": map[string]map[string]any{}}
		if reader != nil && counts["TASK_RUNNING"] > 0 {
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
