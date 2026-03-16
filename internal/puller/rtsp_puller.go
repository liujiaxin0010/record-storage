package puller

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/bluenviron/gortsplib/v4/pkg/format/rtph264"
	"github.com/bluenviron/gortsplib/v4/pkg/format/rtph265"
	"github.com/bluenviron/gortsplib/v4/pkg/format/rtpmpeg4video"
	"github.com/bluenviron/mediacommon/pkg/codecs/h264"
	h265codec "github.com/bluenviron/mediacommon/pkg/codecs/h265"
	"github.com/bluenviron/mediacommon/pkg/codecs/mpeg4video"
	"github.com/pion/rtp"

	"recording-server/internal/recorder"
)

type RTSPPuller struct {
	mu        sync.Mutex
	cameraID  string
	rtspURL   string
	transport string
	client    *gortsplib.Client
	onSample  func(recorder.Sample) error
	onEvent   func(event string, err error)
	stopCh    chan struct{}
	stopped   bool
	wg        sync.WaitGroup
	baseTime  time.Time
}

func NewRTSPPuller(cameraID, rtspURL, transport string, onSample func(recorder.Sample) error, onEvent func(string, error)) *RTSPPuller {
	return &RTSPPuller{
		cameraID:  cameraID,
		rtspURL:   rtspURL,
		transport: strings.ToLower(strings.TrimSpace(transport)),
		onSample:  onSample,
		onEvent:   onEvent,
		stopCh:    make(chan struct{}),
	}
}

func (p *RTSPPuller) Start() error {
	if err := p.connectAndPlay(); err != nil {
		return err
	}
	p.wg.Add(1)
	go p.run()
	return nil
}

func (p *RTSPPuller) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	close(p.stopCh)
	client := p.client
	p.mu.Unlock()
	if client != nil {
		client.Close()
	}
	p.wg.Wait()
}

func (p *RTSPPuller) run() {
	defer p.wg.Done()
	backoff := time.Second
	for {
		p.mu.Lock()
		client := p.client
		stopped := p.stopped
		p.mu.Unlock()
		if stopped || client == nil {
			return
		}
		err := client.Wait()
		select {
		case <-p.stopCh:
			return
		default:
		}
		p.emitEvent("disconnect", err)
		time.Sleep(backoff)
		for {
			if err := p.connectAndPlay(); err == nil {
				p.emitEvent("reconnect", nil)
				backoff = time.Second
				break
			} else {
				p.emitEvent("reconnect_failed", err)
			}
			select {
			case <-p.stopCh:
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
		}
	}
}

func (p *RTSPPuller) connectAndPlay() error {
	u, err := base.ParseURL(p.rtspURL)
	if err != nil {
		return err
	}

	client := &gortsplib.Client{}
	if transport := p.clientTransport(); transport != nil {
		client.Transport = transport
	}
	if err := client.Start(u.Scheme, u.Host); err != nil {
		return err
	}

	desc, _, err := client.Describe(u)
	if err != nil {
		client.Close()
		return err
	}

	media, err := p.setupHandlers(client, desc)
	if err != nil {
		client.Close()
		return err
	}
	if _, err := client.Setup(desc.BaseURL, media, 0, 0); err != nil {
		client.Close()
		return err
	}
	if _, err := client.Play(nil); err != nil {
		client.Close()
		return err
	}

	p.mu.Lock()
	old := p.client
	p.client = client
	p.baseTime = time.Now()
	p.mu.Unlock()
	if old != nil {
		old.Close()
	}
	p.emitEvent("connect", nil)
	return nil
}

func (p *RTSPPuller) emitEvent(event string, err error) {
	if p.onEvent != nil {
		p.onEvent(event, err)
	}
}

func (p *RTSPPuller) setupHandlers(client *gortsplib.Client, desc *description.Session) (*description.Media, error) {
	for _, media := range desc.Medias {
		for _, formaAny := range media.Formats {
			switch forma := formaAny.(type) {
			case *format.H264:
			decoder, err := forma.CreateDecoder()
			if err != nil {
				return nil, err
			}
			client.OnPacketRTP(media, forma, func(pkt *rtp.Packet) {
				p.handleH264(client, media, forma, decoder, pkt)
			})
			return media, nil
			case *format.H265:
			decoder, err := forma.CreateDecoder()
			if err != nil {
				return nil, err
			}
			client.OnPacketRTP(media, forma, func(pkt *rtp.Packet) {
				p.handleH265(client, media, forma, decoder, pkt)
			})
			return media, nil
			case *format.MPEG4Video:
			decoder, err := forma.CreateDecoder()
			if err != nil {
				return nil, err
			}
			client.OnPacketRTP(media, forma, func(pkt *rtp.Packet) {
				p.handleMPEG4(client, media, forma, decoder, pkt)
			})
			return media, nil
			}
		}
	}
	return nil, fmt.Errorf("no supported video format found")
}

func (p *RTSPPuller) handleH264(client *gortsplib.Client, media *description.Media, forma *format.H264, decoder *rtph264.Decoder, pkt *rtp.Packet) {
	pts, ok := client.PacketPTS(media, pkt)
	if !ok {
		return
	}
	aus, err := decoder.Decode(pkt)
	if err != nil {
		if err != rtph264.ErrMorePacketsNeeded && err != rtph264.ErrNonStartingPacketAndNoPrevious {
			return
		}
		return
	}
	params := recorder.CodecParameters{PayloadType: forma.PayloadType(), SPS: append([]byte(nil), forma.SPS...), PPS: append([]byte(nil), forma.PPS...)}
	for _, nalu := range aus {
		typ := h264.NALUType(nalu[0] & 0x1F)
		switch typ {
		case h264.NALUTypeSPS:
			params.SPS = append([]byte(nil), nalu...)
		case h264.NALUTypePPS:
			params.PPS = append([]byte(nil), nalu...)
		}
	}
	_ = p.onSample(recorder.Sample{
		Codec:     recorder.CodecH264,
		Timestamp: p.timestampFromPTS(pts),
		Units:     cloneUnits(aus),
		IsKey:     isH264Key(aus),
		FrameType: frameTypeFromKey(isH264Key(aus)),
		Params:    params,
	})
}

func (p *RTSPPuller) handleH265(client *gortsplib.Client, media *description.Media, forma *format.H265, decoder *rtph265.Decoder, pkt *rtp.Packet) {
	pts, ok := client.PacketPTS(media, pkt)
	if !ok {
		return
	}
	aus, err := decoder.Decode(pkt)
	if err != nil {
		if err != rtph265.ErrMorePacketsNeeded && err != rtph265.ErrNonStartingPacketAndNoPrevious {
			return
		}
		return
	}
	params := recorder.CodecParameters{PayloadType: forma.PayloadType(), VPS: append([]byte(nil), forma.VPS...), SPS: append([]byte(nil), forma.SPS...), PPS: append([]byte(nil), forma.PPS...)}
	for _, nalu := range aus {
		typ := h265codec.NALUType((nalu[0] >> 1) & 0b111111)
		switch typ {
		case h265codec.NALUType_VPS_NUT:
			params.VPS = append([]byte(nil), nalu...)
		case h265codec.NALUType_SPS_NUT:
			params.SPS = append([]byte(nil), nalu...)
		case h265codec.NALUType_PPS_NUT:
			params.PPS = append([]byte(nil), nalu...)
		}
	}
	isKey := isH265Key(aus)
	_ = p.onSample(recorder.Sample{
		Codec:     recorder.CodecH265,
		Timestamp: p.timestampFromPTS(pts),
		Units:     cloneUnits(aus),
		IsKey:     isKey,
		FrameType: frameTypeFromKey(isKey),
		Params:    params,
	})
}

func (p *RTSPPuller) handleMPEG4(client *gortsplib.Client, media *description.Media, forma *format.MPEG4Video, decoder *rtpmpeg4video.Decoder, pkt *rtp.Packet) {
	pts, ok := client.PacketPTS(media, pkt)
	if !ok {
		return
	}
	frame, err := decoder.Decode(pkt)
	if err != nil || len(frame) == 0 {
		return
	}
	isKey := bytes.Contains(frame, []byte{0x00, 0x00, 0x01, byte(mpeg4video.GroupOfVOPStartCode)})
	_ = p.onSample(recorder.Sample{
		Codec:     recorder.CodecMPEG4,
		Timestamp: p.timestampFromPTS(pts),
		Units:     [][]byte{append([]byte(nil), frame...)},
		IsKey:     isKey,
		FrameType: frameTypeFromKey(isKey),
		Params: recorder.CodecParameters{
			PayloadType: forma.PayloadType(),
			Config:      append([]byte(nil), forma.Config...),
		},
	})
}

func (p *RTSPPuller) timestampFromPTS(pts time.Duration) time.Time {
	p.mu.Lock()
	baseTime := p.baseTime
	p.mu.Unlock()
	if baseTime.IsZero() {
		baseTime = time.Now()
	}
	return baseTime.Add(pts)
}

func (p *RTSPPuller) clientTransport() *gortsplib.Transport {
	switch p.transport {
	case "tcp":
		transport := gortsplib.TransportTCP
		return &transport
	case "udp":
		transport := gortsplib.TransportUDP
		return &transport
	default:
		return nil
	}
}

func cloneUnits(units [][]byte) [][]byte {
	cloned := make([][]byte, len(units))
	for i, unit := range units {
		cloned[i] = append([]byte(nil), unit...)
	}
	return cloned
}

func frameTypeFromKey(isKey bool) recorder.FrameType {
	if isKey {
		return recorder.FrameTypeI
	}
	return recorder.FrameTypeP
}

func isH264Key(aus [][]byte) bool {
	for _, nalu := range aus {
		if len(nalu) == 0 {
			continue
		}
		typ := h264.NALUType(nalu[0] & 0x1F)
		if typ == h264.NALUTypeIDR || typ == h264.NALUTypeSPS || typ == h264.NALUTypePPS {
			return true
		}
	}
	return false
}

func isH265Key(aus [][]byte) bool {
	for _, nalu := range aus {
		if len(nalu) < 2 {
			continue
		}
		typ := h265codec.NALUType((nalu[0] >> 1) & 0b111111)
		switch typ {
		case h265codec.NALUType_IDR_W_RADL, h265codec.NALUType_IDR_N_LP, h265codec.NALUType_CRA_NUT,
			h265codec.NALUType_VPS_NUT, h265codec.NALUType_SPS_NUT, h265codec.NALUType_PPS_NUT:
			return true
		}
	}
	return false
}
