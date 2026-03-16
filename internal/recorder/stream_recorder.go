package recorder

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"recording-server/internal/index"
	"recording-server/internal/storage"
)

var errNoGOPAvailable = errors.New("no gop available")

type StreamRecorder struct {
	mu        sync.Mutex
	cameraID  string
	assembler *GOPAssembler
	builder   *FMP4Builder
	writer    *AsyncWriter

	state    atomic.Value
	initKey  string
	initPath string
	initData []byte
	params   CodecParameters
	codec    Codec
	sequence uint64
}

func NewStreamRecorder(cameraID string, writer *AsyncWriter) *StreamRecorder {
	s := &StreamRecorder{
		cameraID:  cameraID,
		assembler: NewGOPAssembler(),
		writer:    writer,
	}
	s.state.Store(RecorderStateIdle)
	return s
}

func (s *StreamRecorder) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.state.Load().(RecorderState)
	if state == RecorderStateRecording || state == RecorderStateWaitingKeyFrame {
		return
	}
	s.state.Store(RecorderStateWaitingKeyFrame)
	if err := s.writer.WriteEvent(s.cameraID, "start", map[string]string{"state": string(RecorderStateWaitingKeyFrame)}); err != nil {
		log.Printf("[%s] failed to write start event: %v", s.cameraID, err)
	}
}

func (s *StreamRecorder) Stop() {
	s.mu.Lock()
	state := s.state.Load().(RecorderState)
	if state == RecorderStateIdle {
		s.mu.Unlock()
		return
	}
	s.state.Store(RecorderStateStopping)
	s.mu.Unlock()
	if err := s.FlushGOP(); err != nil && !errors.Is(err, errNoGOPAvailable) {
		if err := s.writer.WriteEvent(s.cameraID, "stop_error", map[string]string{"error": err.Error()}); err != nil {
			log.Printf("[%s] failed to write stop_error event: %v", s.cameraID, err)
		}
	}
	s.mu.Lock()
	s.state.Store(RecorderStateIdle)
	s.mu.Unlock()
	if err := s.writer.WriteEvent(s.cameraID, "stop", map[string]string{"state": string(RecorderStateIdle)}); err != nil {
		log.Printf("[%s] failed to write stop event: %v", s.cameraID, err)
	}
}

func (s *StreamRecorder) State() RecorderState {
	return s.state.Load().(RecorderState)
}

func (s *StreamRecorder) ProcessSample(sample Sample) error {
	state := s.state.Load().(RecorderState)
	if state == RecorderStateIdle || state == RecorderStateStopping {
		return nil
	}
	if state == RecorderStateWaitingKeyFrame && !sample.IsKey {
		return nil
	}

	transitionedToRecording := false
	if state == RecorderStateWaitingKeyFrame && sample.IsKey {
		s.mu.Lock()
		s.state.Store(RecorderStateRecording)
		s.mu.Unlock()
		transitionedToRecording = true
	}

	s.mu.Lock()
	needsRebuild := s.builder == nil || sample.Codec != s.codec || !ParamsEqual(sample.Params, s.params)
	hadBuilder := s.builder != nil
	s.mu.Unlock()

	if transitionedToRecording {
		if err := s.writer.WriteEvent(s.cameraID, "recording", map[string]string{"codec": string(sample.Codec)}); err != nil {
			log.Printf("[%s] failed to write recording event: %v", s.cameraID, err)
		}
	}

	if needsRebuild {
		if err := s.FlushGOP(); err != nil && !errors.Is(err, errNoGOPAvailable) {
			return err
		}
		if err := s.rebuildBuilder(sample, hadBuilder); err != nil {
			return err
		}
	}

	flush, err := s.assembler.AddSample(sample)
	if err != nil {
		return err
	}
	if flush {
		if err := s.FlushGOP(); err != nil {
			return err
		}
		_, err = s.assembler.AddSample(sample)
		return err
	}
	return nil
}

func (s *StreamRecorder) rebuildBuilder(sample Sample, reportCodecChange bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.builder = NewFMP4Builder(sample.Codec, sample.Params)
	initData, initKey, err := s.builder.BuildInit()
	if err != nil {
		return err
	}
	s.initData = initData
	s.initKey = initKey
	s.initPath = storage.BuildObjectKey(s.cameraID, sample.Timestamp, s.sequence, ".mp4init")
	s.params = sample.Params
	s.codec = sample.Codec
	if reportCodecChange {
		if err := s.writer.WriteEvent(s.cameraID, "codec_change", map[string]string{"codec": string(sample.Codec)}); err != nil {
			log.Printf("[%s] failed to write codec_change event: %v", s.cameraID, err)
		}
	}
	return nil
}

func (s *StreamRecorder) FlushGOP() error {
	samples, startTime, err := s.assembler.Flush()
	if err != nil {
		return errNoGOPAvailable
	}
	if len(samples) == 0 {
		return nil
	}
	s.mu.Lock()
	builder := s.builder
	initData := s.initData
	initPath := s.initPath
	codec := s.codec
	seq := s.sequence
	s.sequence++
	s.initData = nil
	s.mu.Unlock()
	if builder == nil {
		return fmt.Errorf("builder not initialized")
	}
	fragment, err := builder.BuildFragment(samples, startTime)
	if err != nil {
		return err
	}
	endTime := samples[len(samples)-1].Timestamp
	objectKey := storage.BuildObjectKey(s.cameraID, startTime, seq, ".frag")
	return s.writer.Submit(&WriteJob{
		CameraID: s.cameraID,
		GOPData:  fragment,
		InitData: initData,
		Meta: index.GOPMeta{
			CameraID:   s.cameraID,
			StartTime:  startTime,
			EndTime:    endTime,
			ObjectKey:  objectKey,
			InitKey:    initPath,
			Size:       int64(len(fragment)),
			FrameCount: len(samples),
			Codec:      string(codec),
		},
	})
}
