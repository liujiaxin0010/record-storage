package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"recording-server/internal/index"
	"recording-server/internal/recorder"
)

type Server struct {
	index     index.IndexStore
	writer    *recorder.AsyncWriter
	recorders map[string]*recorder.StreamRecorder
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
	cameraID := r.URL.Path[len("/api/v1/recordings/"):]
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active_recorders": activeRecorders,
		"writer": map[string]interface{}{
			"submitted": submitted,
			"failed":    failed,
			"dropped":   dropped,
			"queue_len": queueLen,
		},
	})
}
