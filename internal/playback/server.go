package playback

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/description"

	"recording-server/internal/index"
	"recording-server/internal/recorder"
	"recording-server/internal/storage"
)

type Server struct {
	port      int
	storage   storage.StorageBackend
	index     index.IndexStore
	rtsp      *gortsplib.Server
	reader    *FragmentReader
	mu        sync.Mutex
	pending   map[*gortsplib.ServerConn]*sessionState
	sessions  map[*gortsplib.ServerSession]*sessionState
}

type playbackRequest struct {
	CameraID string
	Start    time.Time
	End      *time.Time
}

type sessionState struct {
	mu              sync.Mutex
	request         playbackRequest
	stream          *gortsplib.ServerStream
	media           interface{}
	init            recorder.InitSegment
	cancel          context.CancelFunc
	lastScale       float64
	currentPosition time.Time
	hasProgress     bool
	prefetcher      *Prefetcher
}

func NewServer(port int, store storage.StorageBackend, idx index.IndexStore) *Server {
	return &Server{
		port:     port,
		storage:  store,
		index:    idx,
		reader:   NewFragmentReader(store, idx),
		pending:  make(map[*gortsplib.ServerConn]*sessionState),
		sessions: make(map[*gortsplib.ServerSession]*sessionState),
	}
}

func (s *Server) Start() error {
	s.rtsp = &gortsplib.Server{
		Handler:        s,
		RTSPAddress:    fmt.Sprintf(":%d", s.port),
		UDPRTPAddress:  fmt.Sprintf(":%d", s.port+1),
		UDPRTCPAddress: fmt.Sprintf(":%d", s.port+2),
	}
	return s.rtsp.Start()
}

func (s *Server) Close() error {
	if s.rtsp != nil {
		s.rtsp.Close()
	}
	return nil
}

func (s *Server) OnConnClose(ctx *gortsplib.ServerHandlerOnConnCloseCtx) {
	s.mu.Lock()
	delete(s.pending, ctx.Conn)
	s.mu.Unlock()
}

func (s *Server) OnSessionClose(ctx *gortsplib.ServerHandlerOnSessionCloseCtx) {
	s.mu.Lock()
	state := s.sessions[ctx.Session]
	delete(s.sessions, ctx.Session)
	s.mu.Unlock()
	if state != nil {
		if state.cancel != nil {
			state.cancel()
		}
		if state.stream != nil {
			state.stream.Close()
		}
	}
}

func (s *Server) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	req, err := parsePlaybackRequest(ctx.Path, ctx.Query)
	if err != nil {
		return &base.Response{StatusCode: base.StatusBadRequest}, nil, err
	}
	init, err := s.lookupInitialInit(req)
	if err != nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	desc, media, err := BuildDescription(init)
	if err != nil {
		return &base.Response{StatusCode: base.StatusInternalServerError}, nil, err
	}
	stream := gortsplib.NewServerStream(s.rtsp, desc)
	s.mu.Lock()
	s.pending[ctx.Conn] = &sessionState{
		request:    req,
		stream:     stream,
		media:      media,
		init:       init,
		lastScale:  1.0,
		prefetcher: NewPrefetcher(s.reader),
	}
	s.mu.Unlock()
	return &base.Response{StatusCode: base.StatusOK}, stream, nil
}

func (s *Server) OnSetup(ctx *gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	s.mu.Lock()
	state := s.pending[ctx.Conn]
	if state != nil {
		s.sessions[ctx.Session] = state
	}
	s.mu.Unlock()
	if state == nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, state.stream, nil
}

func (s *Server) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	s.mu.Lock()
	state := s.sessions[ctx.Session]
	s.mu.Unlock()
	if state == nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil
	}
	startTime, scale, err := state.resolvePlayParameters(ctx.Request)
	if err != nil {
		return &base.Response{StatusCode: base.StatusBadRequest}, err
	}
	if scale < 0 {
		return &base.Response{StatusCode: base.StatusBadRequest}, fmt.Errorf("negative scale is not supported")
	}
	if state.cancel != nil {
		state.cancel()
	}
	state.mu.Lock()
	state.lastScale = scale
	state.currentPosition = startTime
	state.hasProgress = true
	state.mu.Unlock()
	playCtx, cancel := context.WithCancel(context.Background())
	state.cancel = cancel
	go s.runPlayback(playCtx, state, startTime, scale)
	return &base.Response{StatusCode: base.StatusOK}, nil
}

func (s *Server) OnPause(ctx *gortsplib.ServerHandlerOnPauseCtx) (*base.Response, error) {
	s.mu.Lock()
	state := s.sessions[ctx.Session]
	s.mu.Unlock()
	if state != nil && state.cancel != nil {
		state.cancel()
		state.cancel = nil
	}
	return &base.Response{StatusCode: base.StatusOK}, nil
}

func (s *Server) runPlayback(ctx context.Context, state *sessionState, start time.Time, scale float64) {
	metas, err := s.reader.ListGOPs(ctx, state.request.CameraID, start, state.request.End)
	if err != nil {
		return
	}
	media, ok := state.media.(*description.Media)
	if !ok {
		return
	}
	sender, err := NewRTPSender(state.stream, media, state.init)
	if err != nil {
		return
	}
	currentInitKey := ""
	var lastSent time.Time
	for i, meta := range metas {
		if meta.EndTime.Before(start) {
			continue
		}
		if state.request.End != nil && meta.StartTime.After(*state.request.End) {
			return
		}
		if meta.InitKey != currentInitKey {
			init, loadErr := s.reader.LoadInit(ctx, meta.InitKey)
			if loadErr == nil && init.Codec == state.init.Codec {
				sender.init = init
				_ = sender.SendCodecParams(meta.StartTime)
				currentInitKey = meta.InitKey
			}
		}

		samples, cached := state.prefetcher.Get(meta.ObjectKey)
		if !cached {
			samples, err = s.reader.ReadGOP(ctx, meta)
			if err != nil {
				return
			}
		}

		if i+1 < len(metas) {
			go state.prefetcher.Prefetch(context.Background(), metas[i+1])
		}

		for _, sample := range samples {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if sample.Timestamp.Before(start) {
				continue
			}
			if state.request.End != nil && sample.Timestamp.After(*state.request.End) {
				return
			}
			if !shouldSendSample(sample, scale) {
				continue
			}
			if !lastSent.IsZero() {
				delay := time.Duration(float64(sample.Timestamp.Sub(lastSent)) / scale)
				if delay > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(delay):
					}
				}
			}
			if err := sender.SendSample(sample); err != nil {
				return
			}
			lastSent = sample.Timestamp
			state.recordProgress(sample.Timestamp)
		}
	}
}

func (s *Server) lookupInitialInit(req playbackRequest) (recorder.InitSegment, error) {
	metas, err := s.reader.ListGOPs(context.Background(), req.CameraID, req.Start, req.End)
	if err != nil {
		return recorder.InitSegment{}, err
	}
	for _, meta := range metas {
		if meta.EndTime.Before(req.Start) {
			continue
		}
		return s.reader.LoadInit(context.Background(), meta.InitKey)
	}
	return recorder.InitSegment{}, fmt.Errorf("no recording found")
}

func parsePlaybackRequest(pathValue, rawQuery string) (playbackRequest, error) {
	cleanPath := strings.Trim(pathValue, "/")
	parts := strings.Split(cleanPath, "/")
	if len(parts) < 2 || parts[0] != "playback" {
		return playbackRequest{}, fmt.Errorf("invalid playback path")
	}
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return playbackRequest{}, err
	}
	start, err := parseRFC3339(query.Get("start"))
	if err != nil {
		return playbackRequest{}, fmt.Errorf("invalid start: %w", err)
	}
	var end *time.Time
	if rawEnd := query.Get("end"); rawEnd != "" {
		value, err := parseRFC3339(rawEnd)
		if err != nil {
			return playbackRequest{}, fmt.Errorf("invalid end: %w", err)
		}
		end = &value
	}
	return playbackRequest{CameraID: parts[1], Start: start, End: end}, nil
}

func parseRFC3339(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	formats := []string{time.RFC3339Nano, time.RFC3339}
	for _, layout := range formats {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format")
}

func (s *sessionState) resolvePlayParameters(request *base.Request) (time.Time, float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := s.request.Start
	if s.hasProgress {
		start = s.currentPosition
	}
	scale := s.lastScale
	if scale == 0 {
		scale = 1.0
	}
	if values := headerValue(request.Header, "Range"); len(values) > 0 {
		parsed, err := parseRangeHeader(values[0], s.request.Start)
		if err != nil {
			return time.Time{}, 0, err
		}
		start = parsed
	}
	if values := headerValue(request.Header, "Scale"); len(values) > 0 {
		value, err := parseScale(values[0])
		if err != nil {
			return time.Time{}, 0, err
		}
		scale = value
	}
	return start, scale, nil
}

func (s *sessionState) recordProgress(ts time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentPosition = ts
	s.hasProgress = true
}

func headerValue(header base.Header, key string) []string {
	for headerKey, value := range header {
		if strings.EqualFold(headerKey, key) {
			return value
		}
	}
	return nil
}

func parseRangeHeader(value string, baseStart time.Time) (time.Time, error) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(value), "npt=") {
		trimmed := strings.TrimPrefix(strings.ToLower(value), "npt=")
		trimmed = strings.TrimSuffix(trimmed, "-")
		seconds, err := time.ParseDuration(trimmed + "s")
		if err != nil {
			return time.Time{}, err
		}
		return baseStart.Add(seconds), nil
	}
	if strings.HasPrefix(strings.ToLower(value), "clock=") {
		trimmed := strings.TrimPrefix(value, "clock=")
		trimmed = strings.TrimSuffix(trimmed, "-")
		parsed, err := time.Parse("20060102T150405Z", trimmed)
		if err != nil {
			return time.Time{}, err
		}
		return parsed, nil
	}
	return time.Time{}, fmt.Errorf("unsupported range header")
}

func parseScale(value string) (float64, error) {
	value = strings.TrimSpace(value)
	switch value {
	case "0.125":
		return 0.125, nil
	case "0.25":
		return 0.25, nil
	case "0.5":
		return 0.5, nil
	case "1", "1.0", "":
		return 1.0, nil
	case "2", "2.0":
		return 2.0, nil
	case "4", "4.0":
		return 4.0, nil
	case "8", "8.0":
		return 8.0, nil
	case "16", "16.0":
		return 16.0, nil
	default:
		return 0, fmt.Errorf("unsupported scale: %s", value)
	}
}

func shouldSendSample(sample recorder.Sample, scale float64) bool {
	if scale >= 8.0 {
		return sample.IsKey
	}
	if scale >= 4.0 {
		return sample.FrameType != recorder.FrameTypeB
	}
	return true
}
