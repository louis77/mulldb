package pgwire

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// Reader reads PostgreSQL wire protocol messages from a connection.
type Reader struct {
	r *bufio.Reader
}

// NewReader wraps an io.Reader for reading PG protocol messages.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// ReadStartup reads the initial untyped message from the client.
// It returns the parsed StartupMessage and whether the message was an SSL
// request (in which case msg is nil and the caller should refuse SSL and
// call ReadStartup again).
func (r *Reader) ReadStartup() (msg *StartupMessage, isSSL bool, err error) {
	var length int32
	if err := binary.Read(r.r, binary.BigEndian, &length); err != nil {
		return nil, false, fmt.Errorf("read startup length: %w", err)
	}
	if length < 8 {
		return nil, false, fmt.Errorf("startup message too short: %d bytes", length)
	}

	payload := make([]byte, length-4)
	if _, err := io.ReadFull(r.r, payload); err != nil {
		return nil, false, fmt.Errorf("read startup payload: %w", err)
	}

	version := int32(binary.BigEndian.Uint32(payload[:4]))

	if version == SSLRequestCode {
		return nil, true, nil
	}
	if version != ProtocolVersion {
		return nil, false, fmt.Errorf("unsupported protocol version: %d.%d",
			version>>16, version&0xFFFF)
	}

	startup := &StartupMessage{
		ProtocolVersion: version,
		Parameters:      make(map[string]string),
	}
	params := payload[4:]
	for len(params) > 1 {
		key, rest := readCString(params)
		if len(rest) == 0 {
			break
		}
		value, rest := readCString(rest)
		startup.Parameters[key] = value
		params = rest
	}

	return startup, false, nil
}

// ReadMessage reads a typed message (1-byte type + int32 length + payload).
func (r *Reader) ReadMessage() (msgType byte, payload []byte, err error) {
	msgType, err = r.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	var length int32
	if err := binary.Read(r.r, binary.BigEndian, &length); err != nil {
		return 0, nil, fmt.Errorf("read message length: %w", err)
	}
	if length < 4 {
		return 0, nil, fmt.Errorf("message length too short: %d", length)
	}

	payload = make([]byte, length-4)
	if length > 4 {
		if _, err := io.ReadFull(r.r, payload); err != nil {
			return 0, nil, fmt.Errorf("read message payload: %w", err)
		}
	}
	return msgType, payload, nil
}

// readCString reads a null-terminated string from b, returning the string
// and the remaining bytes after the null terminator.
func readCString(b []byte) (string, []byte) {
	for i, c := range b {
		if c == 0 {
			return string(b[:i]), b[i+1:]
		}
	}
	return string(b), nil
}
