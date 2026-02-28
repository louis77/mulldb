package storage

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Value encoding: 1-byte type tag followed by type-specific data.
//
//	tagNull    (0): no data
//	tagInteger (1): 8 bytes int64 big-endian
//	tagText    (2): uint16 length + bytes
//	tagBoolean (3): 1 byte (0=false, 1=true)
const (
	tagNull      byte = 0
	tagInteger   byte = 1
	tagText      byte = 2
	tagBoolean   byte = 3
	tagTimestamp byte = 4
)

// encodeValue appends the binary encoding of v to buf.
func encodeValue(buf []byte, v any) []byte {
	switch val := v.(type) {
	case nil:
		return append(buf, tagNull)
	case int64:
		buf = append(buf, tagInteger)
		return binary.BigEndian.AppendUint64(buf, uint64(val))
	case string:
		buf = append(buf, tagText)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(val)))
		return append(buf, val...)
	case bool:
		buf = append(buf, tagBoolean)
		if val {
			return append(buf, 1)
		}
		return append(buf, 0)
	case time.Time:
		buf = append(buf, tagTimestamp)
		usec := val.UnixMicro()
		return binary.BigEndian.AppendUint64(buf, uint64(usec))
	default:
		// Treat unknown types as NULL.
		return append(buf, tagNull)
	}
}

// decodeValue reads one value from data, returning the value and the
// remaining bytes.
func decodeValue(data []byte) (any, []byte, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty value data")
	}
	tag := data[0]
	data = data[1:]

	switch tag {
	case tagNull:
		return nil, data, nil
	case tagInteger:
		if len(data) < 8 {
			return nil, nil, fmt.Errorf("truncated integer value")
		}
		v := int64(binary.BigEndian.Uint64(data[:8]))
		return v, data[8:], nil
	case tagText:
		if len(data) < 2 {
			return nil, nil, fmt.Errorf("truncated text length")
		}
		n := binary.BigEndian.Uint16(data[:2])
		data = data[2:]
		if len(data) < int(n) {
			return nil, nil, fmt.Errorf("truncated text value")
		}
		return string(data[:n]), data[n:], nil
	case tagBoolean:
		if len(data) < 1 {
			return nil, nil, fmt.Errorf("truncated boolean value")
		}
		return data[0] != 0, data[1:], nil
	case tagTimestamp:
		if len(data) < 8 {
			return nil, nil, fmt.Errorf("truncated timestamp value")
		}
		usec := int64(binary.BigEndian.Uint64(data[:8]))
		return time.UnixMicro(usec).UTC(), data[8:], nil
	default:
		return nil, nil, fmt.Errorf("unknown value tag %d", tag)
	}
}

// encodeValues appends a uint16 count followed by each encoded value.
func encodeValues(buf []byte, values []any) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(values)))
	for _, v := range values {
		buf = encodeValue(buf, v)
	}
	return buf
}

// decodeValues reads a uint16 count and that many values from data.
func decodeValues(data []byte) ([]any, []byte, error) {
	if len(data) < 2 {
		return nil, nil, fmt.Errorf("truncated value count")
	}
	count := binary.BigEndian.Uint16(data[:2])
	data = data[2:]

	values := make([]any, count)
	for i := range values {
		var err error
		values[i], data, err = decodeValue(data)
		if err != nil {
			return nil, nil, fmt.Errorf("value[%d]: %w", i, err)
		}
	}
	return values, data, nil
}

// encodeString appends a uint16-length-prefixed string.
func encodeString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

// decodeString reads a uint16-length-prefixed string.
func decodeString(data []byte) (string, []byte, error) {
	if len(data) < 2 {
		return "", nil, fmt.Errorf("truncated string length")
	}
	n := binary.BigEndian.Uint16(data[:2])
	data = data[2:]
	if len(data) < int(n) {
		return "", nil, fmt.Errorf("truncated string data")
	}
	return string(data[:n]), data[n:], nil
}
