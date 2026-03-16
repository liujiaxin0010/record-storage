package playback

import (
	"fmt"
	"time"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/bluenviron/gortsplib/v4/pkg/rtptime"
	"github.com/pion/rtp"

	"recording-server/internal/recorder"
)

type RTPSender struct {
	stream   *gortsplib.ServerStream
	media    *description.Media
	baseTime time.Time
	rtpTime  *rtptime.Encoder
	init     recorder.InitSegment
	h264Enc  interface{ Encode([][]byte) ([]*rtp.Packet, error) }
	h265Enc  interface{ Encode([][]byte) ([]*rtp.Packet, error) }
	mp4vEnc  interface{ Encode([]byte) ([]*rtp.Packet, error) }
}

func BuildDescription(init recorder.InitSegment) (*description.Session, *description.Media, error) {
	payloadType := init.Params.PayloadType
	if payloadType == 0 {
		payloadType = 96
	}
	var mediaFormat format.Format
	switch init.Codec {
	case recorder.CodecH264:
		mediaFormat = &format.H264{PayloadTyp: payloadType, SPS: init.Params.SPS, PPS: init.Params.PPS, PacketizationMode: 1}
	case recorder.CodecH265:
		mediaFormat = &format.H265{PayloadTyp: payloadType, VPS: init.Params.VPS, SPS: init.Params.SPS, PPS: init.Params.PPS}
	case recorder.CodecMPEG4:
		mediaFormat = &format.MPEG4Video{PayloadTyp: payloadType, Config: init.Params.Config, ProfileLevelID: 1}
	default:
		return nil, nil, fmt.Errorf("unsupported codec: %s", init.Codec)
	}
	media := &description.Media{Type: description.MediaTypeVideo, Formats: []format.Format{mediaFormat}}
	return &description.Session{Medias: []*description.Media{media}}, media, nil
}

func NewRTPSender(stream *gortsplib.ServerStream, media *description.Media, init recorder.InitSegment) (*RTPSender, error) {
	encoder := &rtptime.Encoder{ClockRate: media.Formats[0].ClockRate()}
	if err := encoder.Initialize(); err != nil {
		return nil, err
	}
	sender := &RTPSender{stream: stream, media: media, init: init, rtpTime: encoder}
	switch forma := media.Formats[0].(type) {
	case *format.H264:
		enc, err := forma.CreateEncoder()
		if err != nil {
			return nil, err
		}
		sender.h264Enc = enc
	case *format.H265:
		enc, err := forma.CreateEncoder()
		if err != nil {
			return nil, err
		}
		sender.h265Enc = enc
	case *format.MPEG4Video:
		enc, err := forma.CreateEncoder()
		if err != nil {
			return nil, err
		}
		sender.mp4vEnc = enc
	default:
		return nil, fmt.Errorf("unsupported media format")
	}
	return sender, nil
}

func (s *RTPSender) SendCodecParams(ts time.Time) error {
	switch s.init.Codec {
	case recorder.CodecH264:
		units := make([][]byte, 0, 2)
		if len(s.init.Params.SPS) > 0 {
			units = append(units, s.init.Params.SPS)
		}
		if len(s.init.Params.PPS) > 0 {
			units = append(units, s.init.Params.PPS)
		}
		if len(units) == 0 {
			return nil
		}
		return s.SendSample(recorder.Sample{Codec: recorder.CodecH264, Timestamp: ts, Units: units, IsKey: true, FrameType: recorder.FrameTypeI})
	case recorder.CodecH265:
		units := make([][]byte, 0, 3)
		if len(s.init.Params.VPS) > 0 {
			units = append(units, s.init.Params.VPS)
		}
		if len(s.init.Params.SPS) > 0 {
			units = append(units, s.init.Params.SPS)
		}
		if len(s.init.Params.PPS) > 0 {
			units = append(units, s.init.Params.PPS)
		}
		if len(units) == 0 {
			return nil
		}
		return s.SendSample(recorder.Sample{Codec: recorder.CodecH265, Timestamp: ts, Units: units, IsKey: true, FrameType: recorder.FrameTypeI})
	default:
		return nil
	}
}

func (s *RTPSender) SendSample(sample recorder.Sample) error {
	if s.baseTime.IsZero() {
		s.baseTime = sample.Timestamp
	}
	rtpTimestamp := s.rtpTime.Encode(sample.Timestamp.Sub(s.baseTime))
	var packets []*rtp.Packet
	var err error
	switch sample.Codec {
	case recorder.CodecH264:
		packets, err = s.h264Enc.Encode(sample.Units)
	case recorder.CodecH265:
		packets, err = s.h265Enc.Encode(sample.Units)
	case recorder.CodecMPEG4:
		if len(sample.Units) == 0 {
			return nil
		}
		packets, err = s.mp4vEnc.Encode(sample.Units[0])
	default:
		return fmt.Errorf("unsupported codec: %s", sample.Codec)
	}
	if err != nil {
		return err
	}
	for _, packet := range packets {
		packet.Timestamp = rtpTimestamp
		if err := s.stream.WritePacketRTP(s.media, packet); err != nil {
			return err
		}
	}
	return nil
}

