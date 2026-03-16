package recorder

import (
	"sync"
	"time"
)

var samplePool = sync.Pool{
	New: func() interface{} {
		return &Sample{
			Units: make([][]byte, 0, 4),
		}
	},
}

func acquireSample() *Sample {
	return samplePool.Get().(*Sample)
}

func releaseSample(s *Sample) {
	s.Units = s.Units[:0]
	s.Params.SPS = nil
	s.Params.PPS = nil
	s.Params.VPS = nil
	s.Params.Config = nil
	samplePool.Put(s)
}

type Codec string

const (
	CodecH264  Codec = "h264"
	CodecH265  Codec = "h265"
	CodecMPEG4 Codec = "mpeg4"
)

type FrameType string

const (
	FrameTypeI FrameType = "I"
	FrameTypeP FrameType = "P"
	FrameTypeB FrameType = "B"
)

type RecorderState string

const (
	RecorderStateIdle            RecorderState = "idle"
	RecorderStateWaitingKeyFrame RecorderState = "waiting_key_frame"
	RecorderStateRecording       RecorderState = "recording"
	RecorderStateStopping        RecorderState = "stopping"
)

type CodecParameters struct {
	PayloadType uint8  `json:"payload_type"`
	Width       uint16 `json:"width"`
	Height      uint16 `json:"height"`
	SPS         []byte `json:"sps,omitempty"`
	PPS         []byte `json:"pps,omitempty"`
	VPS         []byte `json:"vps,omitempty"`
	Config      []byte `json:"config,omitempty"`
}

type Sample struct {
	Codec     Codec           `json:"codec"`
	Timestamp time.Time       `json:"timestamp"`
	Units     [][]byte        `json:"units"`
	IsKey     bool            `json:"is_key"`
	FrameType FrameType       `json:"frame_type"`
	Params    CodecParameters `json:"params"`
}

type InitSegment struct {
	Codec  Codec           `json:"codec"`
	Params CodecParameters `json:"params"`
}
