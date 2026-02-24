package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"mulldb/config"
	"mulldb/executor"
	"mulldb/pgwire"
)

// Connection handles the lifecycle of a single client connection:
// startup handshake → authentication → query loop.
type Connection struct {
	conn   net.Conn
	reader *pgwire.Reader
	writer *pgwire.Writer
	cfg    *config.Config
	exec   *executor.Executor
}

func newConnection(conn net.Conn, cfg *config.Config, exec *executor.Executor) *Connection {
	return &Connection{
		conn:   conn,
		reader: pgwire.NewReader(conn),
		writer: pgwire.NewWriter(conn),
		cfg:    cfg,
		exec:   exec,
	}
}

// Handle runs the full connection lifecycle and closes the connection on return.
func (c *Connection) Handle() {
	defer c.conn.Close()

	if err := c.startup(); err != nil {
		log.Printf("connection %s: startup: %v", c.conn.RemoteAddr(), err)
		return
	}

	log.Printf("connection %s: authenticated", c.conn.RemoteAddr())
	c.queryLoop()
	log.Printf("connection %s: disconnected", c.conn.RemoteAddr())
}

// startup performs the PostgreSQL startup handshake and cleartext password
// authentication. It handles optional SSL negotiation.
func (c *Connection) startup() error {
	for {
		msg, isSSL, err := c.reader.ReadStartup()
		if err != nil {
			return fmt.Errorf("read startup: %w", err)
		}
		if isSSL {
			if err := c.writer.WriteSSLRefuse(); err != nil {
				return fmt.Errorf("refuse SSL: %w", err)
			}
			if err := c.writer.Flush(); err != nil {
				return err
			}
			continue
		}

		user := msg.Parameters["user"]
		if user != c.cfg.User {
			c.sendFatalError("28000", fmt.Sprintf("authentication failed for user %q", user))
			return fmt.Errorf("unknown user: %s", user)
		}

		// Request cleartext password.
		if err := c.writer.WriteAuthCleartextPassword(); err != nil {
			return err
		}
		if err := c.writer.Flush(); err != nil {
			return err
		}

		// Read the password response.
		msgType, payload, err := c.reader.ReadMessage()
		if err != nil {
			return fmt.Errorf("read password: %w", err)
		}
		if msgType != pgwire.MsgPasswordMessage {
			return fmt.Errorf("expected PasswordMessage, got '%c'", msgType)
		}

		password := stripNull(payload)
		if password != c.cfg.Password {
			c.sendFatalError("28P01", fmt.Sprintf("password authentication failed for user %q", user))
			return fmt.Errorf("bad password for user: %s", user)
		}

		// Authentication succeeded — send the post-auth preamble.
		if err := c.writer.WriteAuthOk(); err != nil {
			return err
		}
		serverParams := [][2]string{
			{"server_version", "mulldb-0.1"},
			{"server_encoding", "UTF8"},
			{"client_encoding", "UTF8"},
			{"DateStyle", "ISO, MDY"},
		}
		for _, p := range serverParams {
			if err := c.writer.WriteParameterStatus(p[0], p[1]); err != nil {
				return err
			}
		}
		if err := c.writer.WriteBackendKeyData(int32(os.Getpid()), 0); err != nil {
			return err
		}
		if err := c.writer.WriteReadyForQuery(pgwire.TxIdle); err != nil {
			return err
		}
		return c.writer.Flush()
	}
}

// queryLoop reads and responds to client messages until the client
// disconnects or a write error occurs.
func (c *Connection) queryLoop() {
	for {
		msgType, payload, err := c.reader.ReadMessage()
		if err != nil {
			if err != io.EOF {
				log.Printf("connection %s: read: %v", c.conn.RemoteAddr(), err)
			}
			return
		}

		switch msgType {
		case pgwire.MsgQuery:
			query := stripNull(payload)
			if err := c.handleQuery(query); err != nil {
				log.Printf("connection %s: write: %v", c.conn.RemoteAddr(), err)
				return
			}
		case pgwire.MsgTerminate:
			return
		default:
			log.Printf("connection %s: unsupported message type '%c'", c.conn.RemoteAddr(), msgType)
		}
	}
}

// handleQuery processes a single SQL query string and writes the response.
func (c *Connection) handleQuery(query string) error {
	query = strings.TrimSpace(query)

	if query == "" {
		if err := c.writer.WriteEmptyQueryResponse(); err != nil {
			return err
		}
		return c.sendReady()
	}

	// Handle SET commands that psql sends during startup — our parser
	// doesn't cover SET, so we return a stub response.
	if strings.HasPrefix(strings.ToUpper(query), "SET") {
		if err := c.writer.WriteCommandComplete("SET"); err != nil {
			return err
		}
		return c.sendReady()
	}

	// Execute via the real parser + executor + storage path.
	result, err := c.exec.Execute(query)
	if err != nil {
		code := "42000" // fallback
		var qe *executor.QueryError
		if errors.As(err, &qe) {
			code = qe.Code
		}
		if werr := c.writer.WriteErrorResponse("ERROR", code, err.Error()); werr != nil {
			return werr
		}
		return c.sendReady()
	}

	// SELECT: send RowDescription + DataRows + CommandComplete.
	if result.Columns != nil {
		cols := make([]pgwire.ColumnInfo, len(result.Columns))
		for i, rc := range result.Columns {
			cols[i] = pgwire.ColumnInfo{
				Name:         rc.Name,
				DataTypeOID:  rc.TypeOID,
				DataTypeSize: rc.TypeSize,
				TypeModifier: -1,
			}
		}
		if err := c.writer.WriteRowDescription(cols); err != nil {
			return err
		}
		for _, row := range result.Rows {
			if err := c.writer.WriteDataRow(row); err != nil {
				return err
			}
		}
	}

	if err := c.writer.WriteCommandComplete(result.Tag); err != nil {
		return err
	}
	return c.sendReady()
}

// sendReady sends ReadyForQuery and flushes the write buffer.
func (c *Connection) sendReady() error {
	if err := c.writer.WriteReadyForQuery(pgwire.TxIdle); err != nil {
		return err
	}
	return c.writer.Flush()
}

// sendFatalError writes a FATAL error response and flushes. Errors are
// logged but not returned since the connection is about to close.
func (c *Connection) sendFatalError(code, message string) {
	c.writer.WriteErrorResponse("FATAL", code, message)
	c.writer.Flush()
}

// stripNull removes a trailing null byte from the payload, which is how
// the PG protocol terminates strings in most message types.
func stripNull(b []byte) string {
	if len(b) > 0 && b[len(b)-1] == 0 {
		return string(b[:len(b)-1])
	}
	return string(b)
}
