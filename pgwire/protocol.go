package pgwire

// Protocol version 3.0.
const ProtocolVersion int32 = 196608 // 3 << 16

// SSL request code sent by clients before the real startup message.
const SSLRequestCode int32 = 80877103

// Frontend (client → server) message types.
const (
	MsgPasswordMessage byte = 'p'
	MsgQuery           byte = 'Q'
	MsgTerminate       byte = 'X'
)

// Backend (server → client) message types.
const (
	MsgAuthentication     byte = 'R'
	MsgBackendKeyData     byte = 'K'
	MsgCommandComplete    byte = 'C'
	MsgDataRow            byte = 'D'
	MsgErrorResponse      byte = 'E'
	MsgEmptyQueryResponse byte = 'I'
	MsgParameterStatus    byte = 'S'
	MsgReadyForQuery      byte = 'Z'
	MsgRowDescription     byte = 'T'
)

// Authentication sub-types (carried inside 'R' messages).
const (
	AuthOk                int32 = 0
	AuthCleartextPassword int32 = 3
)

// Transaction status indicators for ReadyForQuery.
const (
	TxIdle   byte = 'I'
	TxInTx   byte = 'T'
	TxFailed byte = 'E'
)

// StartupMessage is the initial message sent by the client after the TCP
// connection is established (and after an optional SSL negotiation).
type StartupMessage struct {
	ProtocolVersion int32
	Parameters      map[string]string
}

// ColumnInfo describes a single column in a RowDescription message.
type ColumnInfo struct {
	Name         string
	TableOID     int32
	ColumnAttr   int16
	DataTypeOID  int32
	DataTypeSize int16
	TypeModifier int32
	FormatCode   int16
}
