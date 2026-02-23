package pgwire

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Writer writes PostgreSQL wire protocol messages to a connection.
type Writer struct {
	w   *bufio.Writer
	buf []byte
}

// NewWriter wraps an io.Writer for writing PG protocol messages.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:   bufio.NewWriter(w),
		buf: make([]byte, 0, 1024),
	}
}

// Flush flushes buffered data to the underlying writer.
func (w *Writer) Flush() error {
	return w.w.Flush()
}

// WriteSSLRefuse writes a single 'N' byte to refuse an SSL connection.
func (w *Writer) WriteSSLRefuse() error {
	_, err := w.w.Write([]byte{'N'})
	return err
}

// WriteAuthCleartextPassword tells the client to send a cleartext password.
func (w *Writer) WriteAuthCleartextPassword() error {
	w.beginMessage(MsgAuthentication)
	w.writeInt32(AuthCleartextPassword)
	return w.finishMessage()
}

// WriteAuthOk tells the client that authentication succeeded.
func (w *Writer) WriteAuthOk() error {
	w.beginMessage(MsgAuthentication)
	w.writeInt32(AuthOk)
	return w.finishMessage()
}

// WriteParameterStatus sends a server parameter to the client.
func (w *Writer) WriteParameterStatus(name, value string) error {
	w.beginMessage(MsgParameterStatus)
	w.writeCString(name)
	w.writeCString(value)
	return w.finishMessage()
}

// WriteBackendKeyData sends the backend process ID and secret key.
func (w *Writer) WriteBackendKeyData(pid, secret int32) error {
	w.beginMessage(MsgBackendKeyData)
	w.writeInt32(pid)
	w.writeInt32(secret)
	return w.finishMessage()
}

// WriteReadyForQuery signals the client that the server is ready for a new query.
func (w *Writer) WriteReadyForQuery(status byte) error {
	w.beginMessage(MsgReadyForQuery)
	w.buf = append(w.buf, status)
	return w.finishMessage()
}

// WriteRowDescription sends column metadata for a query result.
func (w *Writer) WriteRowDescription(columns []ColumnInfo) error {
	w.beginMessage(MsgRowDescription)
	w.writeInt16(int16(len(columns)))
	for _, col := range columns {
		w.writeCString(col.Name)
		w.writeInt32(col.TableOID)
		w.writeInt16(col.ColumnAttr)
		w.writeInt32(col.DataTypeOID)
		w.writeInt16(col.DataTypeSize)
		w.writeInt32(col.TypeModifier)
		w.writeInt16(col.FormatCode)
	}
	return w.finishMessage()
}

// WriteDataRow sends a single data row. Each value is text-encoded; nil means NULL.
func (w *Writer) WriteDataRow(values [][]byte) error {
	w.beginMessage(MsgDataRow)
	w.writeInt16(int16(len(values)))
	for _, v := range values {
		if v == nil {
			w.writeInt32(-1)
		} else {
			w.writeInt32(int32(len(v)))
			w.buf = append(w.buf, v...)
		}
	}
	return w.finishMessage()
}

// WriteCommandComplete signals that a command has finished.
func (w *Writer) WriteCommandComplete(tag string) error {
	w.beginMessage(MsgCommandComplete)
	w.writeCString(tag)
	return w.finishMessage()
}

// WriteEmptyQueryResponse signals that an empty query string was received.
func (w *Writer) WriteEmptyQueryResponse() error {
	w.beginMessage(MsgEmptyQueryResponse)
	return w.finishMessage()
}

// WriteErrorResponse sends an error to the client.
func (w *Writer) WriteErrorResponse(severity, code, message string) error {
	w.beginMessage(MsgErrorResponse)
	w.buf = append(w.buf, 'S')
	w.writeCString(severity)
	w.buf = append(w.buf, 'C')
	w.writeCString(code)
	w.buf = append(w.buf, 'M')
	w.writeCString(message)
	w.buf = append(w.buf, 0) // field terminator
	return w.finishMessage()
}

// beginMessage starts building a new message with the given type byte.
func (w *Writer) beginMessage(msgType byte) {
	w.buf = w.buf[:0]
	w.buf = append(w.buf, msgType)
	w.buf = append(w.buf, 0, 0, 0, 0) // length placeholder
}

// finishMessage patches the length field and writes the message to the buffer.
func (w *Writer) finishMessage() error {
	length := int32(len(w.buf) - 1) // length includes itself but not the type byte
	binary.BigEndian.PutUint32(w.buf[1:5], uint32(length))
	_, err := w.w.Write(w.buf)
	return err
}

func (w *Writer) writeInt32(v int32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
}

func (w *Writer) writeInt16(v int16) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v))
}

func (w *Writer) writeCString(s string) {
	w.buf = append(w.buf, s...)
	w.buf = append(w.buf, 0)
}
