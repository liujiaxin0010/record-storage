package playback

import (
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v4/pkg/base"

	"recording-server/internal/recorder"
)

func TestParseRangeAndScale(t *testing.T) {
	startBase := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	target, err := parseRangeHeader("npt=3600.0-", startBase)
	if err != nil {
		t.Fatal(err)
	}
	if !target.Equal(startBase.Add(time.Hour)) {
		t.Fatalf("unexpected npt target: %v", target)
	}
	target, err = parseRangeHeader("clock=20260306T130000Z-", startBase)
	if err != nil {
		t.Fatal(err)
	}
	if target.Hour() != 13 {
		t.Fatalf("unexpected clock target: %v", target)
	}
	scale, err := parseScale("4.0")
	if err != nil || scale != 4 {
		t.Fatalf("unexpected scale: %v %v", scale, err)
	}
	if shouldSendSample(recorder.Sample{IsKey: false, FrameType: recorder.FrameTypeP}, 8.0) {
		t.Fatal("non-key sample should be skipped at 8x")
	}
}

func TestResolvePlayParametersResumePosition(t *testing.T) {
	startBase := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	state := &sessionState{
		request:         playbackRequest{CameraID: "cam01", Start: startBase},
		lastScale:       2.0,
		currentPosition: startBase.Add(15 * time.Second),
		hasProgress:     true,
	}
	start, scale, err := state.resolvePlayParameters(&base.Request{Header: base.Header{}})
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(startBase.Add(15 * time.Second)) {
		t.Fatalf("expected resume position, got %v", start)
	}
	if scale != 2.0 {
		t.Fatalf("expected last scale, got %v", scale)
	}

	start, scale, err = state.resolvePlayParameters(&base.Request{Header: base.Header{
		"Range": base.HeaderValue{"npt=5.0-"},
		"Scale": base.HeaderValue{"4.0"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(startBase.Add(5 * time.Second)) {
		t.Fatalf("expected range override, got %v", start)
	}
	if scale != 4.0 {
		t.Fatalf("expected scale override, got %v", scale)
	}
}
