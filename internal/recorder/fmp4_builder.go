package recorder

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/Eyevinn/mp4ff/mp4"
)

type FMP4Builder struct {
	codec    Codec
	params   CodecParameters
	sequence uint32
}

const (
	fmp4TrackID   uint32 = 1
	fmp4Timescale uint32 = 90000
	initMetaBox          = "rinf"
)

func NewFMP4Builder(codec Codec, params CodecParameters) *FMP4Builder {
	return &FMP4Builder{codec: codec, params: params}
}

func (f *FMP4Builder) BuildInit() ([]byte, string, error) {
	payload, err := json.Marshal(InitSegment{Codec: f.codec, Params: f.params})
	if err != nil {
		return nil, "", err
	}
	standard, err := f.buildStandardInit()
	if err != nil {
		return nil, "", err
	}
	buf := bytes.NewBuffer(standard)
	writeBox(buf, initMetaBox, payload)
	hash := sha1.Sum(payload)
	return buf.Bytes(), fmt.Sprintf("%x", hash[:8]), nil
}

func (f *FMP4Builder) BuildFragment(samples []Sample, _ time.Time) ([]byte, error) {
	if len(samples) == 0 {
		return nil, fmt.Errorf("no samples")
	}
	f.sequence++
	fragment, err := mp4.CreateFragment(f.sequence, fmp4TrackID)
	if err != nil {
		return nil, err
	}
	durations := computeSampleDurations(samples)
	var decodeTime uint64
	for index, sample := range samples {
		data, err := encodeSampleData(f.codec, sample.Units)
		if err != nil {
			return nil, err
		}
		flags := mp4.NonSyncSampleFlags
		if sample.IsKey {
			flags = mp4.SyncSampleFlags
		}
		fragment.AddFullSample(mp4.FullSample{
			Sample: mp4.Sample{
				Flags: flags,
				Dur:   durations[index],
				Size:  uint32(len(data)),
			},
			DecodeTime: decodeTime,
			Data:       data,
		})
		decodeTime += uint64(durations[index])
	}
	segment := mp4.NewMediaSegmentWithoutStyp()
	segment.AddFragment(fragment)
	buf := bytes.NewBuffer(nil)
	if err := segment.Encode(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ParseInitSegment(data []byte) (InitSegment, error) {
	boxes, err := parseBoxes(data)
	if err != nil {
		return InitSegment{}, err
	}
	payload, ok := boxes[initMetaBox]
	if !ok {
		payload, ok = boxes["init"]
	}
	if !ok {
		return InitSegment{}, fmt.Errorf("missing init metadata")
	}
	var segment InitSegment
	if err := json.Unmarshal(payload, &segment); err != nil {
		return InitSegment{}, err
	}
	return segment, nil
}

func ParseFragment(data []byte, codec Codec, startTime time.Time) ([]Sample, error) {
	if samples, err := parseActualFragment(data, codec, startTime); err == nil {
		return samples, nil
	}
	return parseLegacyFragment(data)
}

func ParamsEqual(left, right CodecParameters) bool {
	return bytes.Equal(left.SPS, right.SPS) &&
		bytes.Equal(left.PPS, right.PPS) &&
		bytes.Equal(left.VPS, right.VPS) &&
		bytes.Equal(left.Config, right.Config) &&
		left.PayloadType == right.PayloadType &&
		left.Width == right.Width &&
		left.Height == right.Height
}

func writeBox(buf *bytes.Buffer, name string, payload []byte) {
	size := uint32(8 + len(payload))
	_ = binary.Write(buf, binary.BigEndian, size)
	buf.WriteString(name)
	buf.Write(payload)
}

func parseBoxes(data []byte) (map[string][]byte, error) {
	result := make(map[string][]byte)
	for len(data) >= 8 {
		size := binary.BigEndian.Uint32(data[:4])
		if size < 8 || int(size) > len(data) {
			return nil, fmt.Errorf("invalid box size")
		}
		name := string(data[4:8])
		result[name] = append([]byte(nil), data[8:size]...)
		data = data[size:]
	}
	return result, nil
}

func (f *FMP4Builder) buildStandardInit() ([]byte, error) {
	init := mp4.CreateEmptyInit()
	init.AddEmptyTrack(fmp4Timescale, "video", "und")
	trak := init.Moov.Trak
	switch f.codec {
	case CodecH264:
		if len(f.params.SPS) == 0 || len(f.params.PPS) == 0 {
			return nil, fmt.Errorf("missing H264 SPS/PPS")
		}
		if err := trak.SetAVCDescriptor("avc3", [][]byte{f.params.SPS}, [][]byte{f.params.PPS}, true); err != nil {
			return nil, err
		}
	case CodecH265:
		if len(f.params.VPS) == 0 || len(f.params.SPS) == 0 || len(f.params.PPS) == 0 {
			return nil, fmt.Errorf("missing H265 VPS/SPS/PPS")
		}
		if err := trak.SetHEVCDescriptor("hvc1", [][]byte{f.params.VPS}, [][]byte{f.params.SPS}, [][]byte{f.params.PPS}, nil, true); err != nil {
			return nil, err
		}
	case CodecMPEG4:
		width := f.params.Width
		height := f.params.Height
		if width == 0 {
			width = 1920
		}
		if height == 0 {
			height = 1080
		}
		entry := mp4.CreateVisualSampleEntryBox("mp4v", width, height, mp4.CreateEsdsBox(f.params.Config))
		trak.Tkhd.Width = mp4.Fixed32(width << 16)
		trak.Tkhd.Height = mp4.Fixed32(height << 16)
		trak.Mdia.Minf.Stbl.Stsd.AddChild(entry)
	default:
		return nil, fmt.Errorf("unsupported codec: %s", f.codec)
	}
	buf := bytes.NewBuffer(nil)
	if err := init.Encode(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func parseActualFragment(data []byte, codec Codec, startTime time.Time) ([]Sample, error) {
	file, err := mp4.DecodeFile(bytes.NewReader(data))
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(file.Segments) == 0 || len(file.Segments[0].Fragments) == 0 {
		return nil, fmt.Errorf("no fragments found")
	}
	fullSamples, err := file.Segments[0].Fragments[0].GetFullSamples(nil)
	if err != nil {
		return nil, err
	}
	samples := make([]Sample, 0, len(fullSamples))
	for _, fullSample := range fullSamples {
		units, err := decodeSampleData(codec, fullSample.Data)
		if err != nil {
			return nil, err
		}
		isKey := fullSample.IsSync()
		frameType := FrameTypeP
		if isKey {
			frameType = FrameTypeI
		}
		timestamp := startTime.Add(time.Duration(fullSample.PresentationTime()) * time.Second / time.Duration(fmp4Timescale))
		samples = append(samples, Sample{
			Codec:     codec,
			Timestamp: timestamp,
			Units:     units,
			IsKey:     isKey,
			FrameType: frameType,
		})
	}
	return samples, nil
}

func parseLegacyFragment(data []byte) ([]Sample, error) {
	boxes, err := parseBoxes(data)
	if err != nil {
		return nil, err
	}
	payload, ok := boxes["moof"]
	if !ok {
		return nil, fmt.Errorf("missing moof box")
	}
	var fragment struct {
		Codec   Codec   `json:"codec"`
		Samples []Sample `json:"samples"`
	}
	if err := json.Unmarshal(payload, &fragment); err != nil {
		return nil, err
	}
	return fragment.Samples, nil
}

func computeSampleDurations(samples []Sample) []uint32 {
	durations := make([]uint32, len(samples))
	for index := range samples {
		switch {
		case len(samples) == 1:
			durations[index] = 3600
		case index < len(samples)-1:
			delta := samples[index+1].Timestamp.Sub(samples[index].Timestamp)
			if delta <= 0 {
				durations[index] = 3600
			} else {
				durations[index] = uint32(delta.Nanoseconds() * int64(fmp4Timescale) / int64(time.Second))
			}
		case index > 0:
			durations[index] = durations[index-1]
		default:
			durations[index] = 3600
		}
		if durations[index] == 0 {
			durations[index] = 1
		}
	}
	return durations
}

func encodeSampleData(codec Codec, units [][]byte) ([]byte, error) {
	switch codec {
	case CodecH264, CodecH265:
		buf := bytes.NewBuffer(nil)
		for _, unit := range units {
			if err := binary.Write(buf, binary.BigEndian, uint32(len(unit))); err != nil {
				return nil, err
			}
			buf.Write(unit)
		}
		return buf.Bytes(), nil
	case CodecMPEG4:
		if len(units) == 0 {
			return nil, fmt.Errorf("no MPEG4 frame")
		}
		return append([]byte(nil), units[0]...), nil
	default:
		return nil, fmt.Errorf("unsupported codec: %s", codec)
	}
}

func decodeSampleData(codec Codec, data []byte) ([][]byte, error) {
	switch codec {
	case CodecH264, CodecH265:
		var units [][]byte
		for len(data) >= 4 {
			size := binary.BigEndian.Uint32(data[:4])
			data = data[4:]
			if size == 0 || int(size) > len(data) {
				return nil, fmt.Errorf("invalid sample NALU size")
			}
			units = append(units, append([]byte(nil), data[:size]...))
			data = data[size:]
		}
		if len(data) != 0 {
			return nil, fmt.Errorf("trailing sample data")
		}
		return units, nil
	case CodecMPEG4:
		return [][]byte{append([]byte(nil), data...)}, nil
	default:
		return nil, fmt.Errorf("unsupported codec: %s", codec)
	}
}
