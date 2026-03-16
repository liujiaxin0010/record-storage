package recorder

import (
	"fmt"
	"sync"
	"time"

	"recording-server/internal/config"
)

type ScheduleController struct {
	mu       sync.Mutex
	entries  []scheduleEntry
	tick     time.Duration
	now      func() time.Time
	stopCh   chan struct{}
	stopped  bool
	started  bool
	wg       sync.WaitGroup
}

type scheduleEntry struct {
	cameraID string
	config   []config.ScheduleConfig
	recorder *StreamRecorder
}

func NewScheduleController(cameras []config.CameraConfig, recorders map[string]*StreamRecorder) *ScheduleController {
	entries := make([]scheduleEntry, 0, len(cameras))
	for _, camera := range cameras {
		if len(camera.Recording.Schedule) == 0 {
			continue
		}
		rec, ok := recorders[camera.ID]
		if !ok {
			continue
		}
		entries = append(entries, scheduleEntry{cameraID: camera.ID, config: camera.Recording.Schedule, recorder: rec})
	}
	return &ScheduleController{
		entries: entries,
		tick:    30 * time.Second,
		now:     time.Now,
		stopCh:  make(chan struct{}),
	}
}

func (s *ScheduleController) Start() {
	s.mu.Lock()
	if s.started || len(s.entries) == 0 {
		s.started = true
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	s.evaluateAll()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.tick)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				s.evaluateAll()
			}
		}
	}()
}

func (s *ScheduleController) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	close(s.stopCh)
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *ScheduleController) Evaluate(now time.Time) {
	for _, entry := range s.entries {
		active := false
		for _, item := range entry.config {
			if scheduleMatches(item, now) {
				active = true
				break
			}
		}
		if active {
			if state := entry.recorder.State(); state == RecorderStateIdle {
				entry.recorder.Start()
			}
		} else {
			if state := entry.recorder.State(); state != RecorderStateIdle {
				entry.recorder.Stop()
			}
		}
	}
}

func (s *ScheduleController) evaluateAll() {
	s.Evaluate(s.now())
}

func scheduleMatches(item config.ScheduleConfig, now time.Time) bool {
	startMinutes, err := parseClockMinutes(item.Start)
	if err != nil {
		return false
	}
	endMinutes, err := parseClockMinutes(item.End)
	if err != nil {
		return false
	}
	currentMinutes := now.Hour()*60 + now.Minute()
	today := isoWeekday(now.Weekday())
	if startMinutes <= endMinutes {
		return containsWeekday(item.Weekdays, today) && currentMinutes >= startMinutes && currentMinutes < endMinutes
	}
	if containsWeekday(item.Weekdays, today) && currentMinutes >= startMinutes {
		return true
	}
	yesterday := isoWeekday(now.Add(-24 * time.Hour).Weekday())
	return containsWeekday(item.Weekdays, yesterday) && currentMinutes < endMinutes
}

func parseClockMinutes(value string) (int, error) {
	parsed, err := time.Parse("15:04", value)
	if err != nil {
		return 0, fmt.Errorf("parse clock %q: %w", value, err)
	}
	return parsed.Hour()*60 + parsed.Minute(), nil
}

func isoWeekday(weekday time.Weekday) int {
	if weekday == time.Sunday {
		return 7
	}
	return int(weekday)
}

func containsWeekday(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
		if value == 0 && target == 7 {
			return true
		}
	}
	return false
}
