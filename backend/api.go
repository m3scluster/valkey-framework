package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (s *Scheduler) handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok\n")) })
	m.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"framework_id": s.frameworkID, "scheduler_connected": s.schedulerConnected, "desired": s.desired, "masters": s.desiredMasterCount(), "slaves": s.cfg.Slaves, "min_masters": minMasters, "min_slaves": minSlaves, "warnings": s.warningsLocked(), "tasks": s.tasks})
	})
	m.HandleFunc("/api/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		counts := map[string]int{}
		if s.schedulerConnected {
			for _, task := range s.tasks {
				counts[task.State]++
			}
		}
		frameworkID, connected, desired, reader := s.frameworkID, s.schedulerConnected, s.desired, s.metricsReader
		s.mu.Unlock()
		live := counts["TASK_RUNNING"] + counts["TASK_STAGING"] + counts["TASK_STARTING"] + counts["TASK_UNKNOWN"]
		metrics := map[string]any{
			"framework_id":        frameworkID,
			"scheduler_connected": connected,
			"desired":             desired,
			"total":               live,
			"running":             counts["TASK_RUNNING"],
			"staging":             counts["TASK_STAGING"],
			"failed":              counts["TASK_FAILED"] + counts["TASK_LOST"],
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
