package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"recording-server/internal/index"
	"recording-server/internal/memory"
	"recording-server/internal/recorder"
)

type Server struct {
	index     index.IndexStore
	writer    *recorder.AsyncWriter
	recorders map[string]*recorder.StreamRecorder
	memCtrl   *memory.Controller
	srv       *http.Server
}

func NewServer(port int, idx index.IndexStore, writer *recorder.AsyncWriter, recorders map[string]*recorder.StreamRecorder) *Server {
	s := &Server{
		index:     idx,
		writer:    writer,
		recorders: recorders,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/recordings/", s.handleRecordings)
	mux.HandleFunc("/api/v1/system/stats", s.handleStats)
	s.srv = &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	return s
}

// SetMemoryController attaches a memory controller for system stats reporting.
func (s *Server) SetMemoryController(mc *memory.Controller) {
	s.memCtrl = mc
}

func (s *Server) Start() error {
	go s.srv.ListenAndServe()
	return nil
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleRecordings(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/recordings/"):]

	// Route: POST /api/v1/recordings/{camera_id}/start
	if strings.HasSuffix(path, "/start") && r.Method == http.MethodPost {
		cameraID := strings.TrimSuffix(path, "/start")
		s.handleStartRecording(w, r, cameraID)
		return
	}

	// Route: POST /api/v1/recordings/{camera_id}/stop
	if strings.HasSuffix(path, "/stop") && r.Method == http.MethodPost {
		cameraID := strings.TrimSuffix(path, "/stop")
		s.handleStopRecording(w, r, cameraID)
		return
	}

	// Route: GET /api/v1/recordings/{camera_id}/status
	if strings.HasSuffix(path, "/status") && r.Method == http.MethodGet {
		cameraID := strings.TrimSuffix(path, "/status")
		s.handleRecordingStatus(w, r, cameraID)
		return
	}

	// Route: GET /api/v1/recordings/{camera_id}?start=...&end=...
	if r.Method == http.MethodGet {
		s.handleRecordingRanges(w, r, path)
		return
	}

	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) handleStartRecording(w http.ResponseWriter, r *http.Request, cameraID string) {
	rec, ok := s.recorders[cameraID]
	if !ok {
		http.Error(w, fmt.Sprintf("camera %s not found", cameraID), http.StatusNotFound)
		return
	}
	rec.Start()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"camera_id": cameraID,
		"state":     string(rec.State()),
	})
}

func (s *Server) handleStopRecording(w http.ResponseWriter, r *http.Request, cameraID string) {
	rec, ok := s.recorders[cameraID]
	if !ok {
		http.Error(w, fmt.Sprintf("camera %s not found", cameraID), http.StatusNotFound)
		return
	}
	rec.Stop()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"camera_id": cameraID,
		"state":     string(rec.State()),
	})
}

func (s *Server) handleRecordingStatus(w http.ResponseWriter, r *http.Request, cameraID string) {
	rec, ok := s.recorders[cameraID]
	if !ok {
		http.Error(w, fmt.Sprintf("camera %s not found", cameraID), http.StatusNotFound)
		return
	}

	// Get today's recording ranges for stats
	now := time.Now()
	todayRanges, _ := s.index.GetRecordingRanges(r.Context(), cameraID, now)
	var totalDuration time.Duration
	for _, tr := range todayRanges {
		totalDuration += tr.End.Sub(tr.Start)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"camera_id": cameraID,
		"state":     string(rec.State()),
		"today": map[string]interface{}{
			"segments":       len(todayRanges),
			"total_duration": totalDuration.String(),
		},
	})
}

func (s *Server) handleRecordingRanges(w http.ResponseWriter, r *http.Request, cameraID string) {
	if cameraID == "" {
		http.Error(w, "camera_id required", http.StatusBadRequest)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	if startStr == "" || endStr == "" {
		http.Error(w, "start and end required", http.StatusBadRequest)
		return
	}

	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		http.Error(w, "invalid start time", http.StatusBadRequest)
		return
	}
	end, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		http.Error(w, "invalid end time", http.StatusBadRequest)
		return
	}

	ranges, err := s.index.GetRecordingRanges(r.Context(), cameraID, start)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	filtered := []index.TimeRange{}
	for _, tr := range ranges {
		if tr.End.After(start) && tr.Start.Before(end) {
			filtered = append(filtered, tr)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ranges": filtered})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	submitted, failed, dropped, queueLen := s.writer.Stats()

	activeRecorders := 0
	for _, rec := range s.recorders {
		if rec.State() == recorder.RecorderStateRecording {
			activeRecorders++
		}
	}

	stats := map[string]interface{}{
		"active_recorders": activeRecorders,
		"writer": map[string]interface{}{
			"submitted": submitted,
			"failed":    failed,
			"dropped":   dropped,
			"queue_len": queueLen,
		},
	}

	if s.memCtrl != nil {
		stats["memory"] = map[string]interface{}{
			"used_bytes":     s.memCtrl.Used(),
			"max_bytes":      s.memCtrl.Max(),
			"pressure_level": s.memCtrl.GetPressureLevel(),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
