package corebgp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// The tests in this file use net.Pipe() as the peer connection. An unbuffered
// pipe blocks a write the moment the remote end stops reading, and it
// serialises writes behind a mutex held for the duration of a write, which is
// the property a real connection's per-fd write lock has. Closing either end
// frees a write blocked on the pipe, again as a socket does.

const (
	testLocalAS  = 65001
	testRemoteAS = 65002
	// teardownTimeout is the budget for a teardown that should complete within
	// closeGracePeriod plus scheduling slack.
	teardownTimeout = time.Second * 10
	// wedgeSettleTime is how long a write is given to reach the connection
	// before it is considered blocked.
	wedgeSettleTime = time.Millisecond * 200
)

// pipeAddr is a net.Addr carrying an arbitrary address string.
type pipeAddr struct {
	addr string
}

func (p pipeAddr) Network() string { return "pipe" }

func (p pipeAddr) String() string { return p.addr }

// pipeConn is a net.Pipe endpoint reporting a remote address that
// Server.handleInboundConn can match against a peer.
type pipeConn struct {
	net.Conn
	remote net.Addr
}

func (p *pipeConn) RemoteAddr() net.Addr { return p.remote }

// pipeListener yields a single connection and then blocks in Accept until it
// is closed.
type pipeListener struct {
	connCh    chan net.Conn
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newPipeListener(conn net.Conn) *pipeListener {
	l := &pipeListener{
		connCh:  make(chan net.Conn, 1),
		closeCh: make(chan struct{}),
	}
	l.connCh <- conn
	return l
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.connCh:
		return c, nil
	case <-l.closeCh:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closeCh)
	})
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr{addr: "pipe"} }

// teardownPlugin hands the test the UpdateMessageWriter for an established
// session and reports when the session closes.
type teardownPlugin struct {
	establishedCh chan UpdateMessageWriter
	closedCh      chan struct{}
	closedOnce    sync.Once
}

func (t *teardownPlugin) GetCapabilities(PeerConfig) []Capability { return nil }

func (t *teardownPlugin) OnOpenMessage(PeerConfig, netip.Addr,
	[]Capability) *Notification {
	return nil
}

func (t *teardownPlugin) OnEstablished(_ PeerConfig,
	writer UpdateMessageWriter) UpdateMessageHandler {
	t.establishedCh <- writer
	return nil
}

func (t *teardownPlugin) OnClose(PeerConfig) {
	t.closedOnce.Do(func() {
		close(t.closedCh)
	})
}

// teardownHarness is a Server with a single passive peer reachable over a
// net.Pipe, together with the peer end of that pipe.
type teardownHarness struct {
	server   *Server
	plugin   *teardownPlugin
	peerAddr netip.Addr
	peerConn net.Conn
	listener *pipeListener
	serveCh  chan error
}

func newTeardownHarness(t *testing.T) *teardownHarness {
	t.Helper()

	localConn, peerConn := net.Pipe()
	peerAddr := netip.MustParseAddr("192.0.2.2")
	listener := newPipeListener(&pipeConn{
		Conn:   localConn,
		remote: pipeAddr{addr: net.JoinHostPort(peerAddr.String(), "179")},
	})

	server, err := NewServer(netip.MustParseAddr("192.0.2.1"))
	if err != nil {
		t.Fatalf("error creating server: %v", err)
	}
	plugin := &teardownPlugin{
		establishedCh: make(chan UpdateMessageWriter, 1),
		closedCh:      make(chan struct{}),
	}
	err = server.AddPeer(PeerConfig{
		RemoteAddress: peerAddr,
		LocalAS:       testLocalAS,
		RemoteAS:      testRemoteAS,
	}, plugin, WithPassive())
	if err != nil {
		t.Fatalf("error adding peer: %v", err)
	}

	h := &teardownHarness{
		server:   server,
		plugin:   plugin,
		peerAddr: peerAddr,
		peerConn: peerConn,
		listener: listener,
		serveCh:  make(chan error, 1),
	}
	go func() {
		h.serveCh <- server.Serve([]net.Listener{listener})
	}()
	t.Cleanup(h.cleanup)
	return h
}

// cleanup closes the peer end first so that any write still wedged in the FSM
// is freed. Cleanup therefore completes even when the test has already failed
// because teardown could not free the write itself.
func (h *teardownHarness) cleanup() {
	h.peerConn.Close()
	h.server.Close()
	h.listener.Close()
}

// readMessage reads a single BGP message from the peer end of the pipe and
// returns its type and body.
func (h *teardownHarness) readMessage() (uint8, []byte, error) {
	header := make([]byte, headerLength)
	if _, err := io.ReadFull(h.peerConn, header); err != nil {
		return 0, nil, err
	}
	msgLen := int(binary.BigEndian.Uint16(header[16:18]))
	if msgLen < headerLength || msgLen > maxMessageLength {
		return 0, nil, fmt.Errorf("invalid message length: %d", msgLen)
	}
	body := make([]byte, msgLen-headerLength)
	if _, err := io.ReadFull(h.peerConn, body); err != nil {
		return 0, nil, err
	}
	return header[18], body, nil
}

func (h *teardownHarness) expectMessage(t *testing.T, want uint8) []byte {
	t.Helper()

	typ, body, err := h.readMessage()
	if err != nil {
		t.Fatalf("error reading message of type %d: %v", want, err)
	}
	if typ != want {
		t.Fatalf("read message of type %d, want %d", typ, want)
	}
	return body
}

// establish drives an OPEN/KEEPALIVE handshake from the peer end of the pipe
// and returns the writer for the resulting session. The peer performs no reads
// after it returns.
func (h *teardownHarness) establish(t *testing.T) UpdateMessageWriter {
	t.Helper()

	h.expectMessage(t, openMessageType)

	open, err := newOpenMessage(testRemoteAS,
		time.Duration(DefaultHoldTimeSeconds)*time.Second, 0x02020202, nil)
	if err != nil {
		t.Fatalf("error building open message: %v", err)
	}
	b, err := open.encode()
	if err != nil {
		t.Fatalf("error encoding open message: %v", err)
	}
	if _, err = h.peerConn.Write(b); err != nil {
		t.Fatalf("error writing open message: %v", err)
	}

	h.expectMessage(t, keepAliveMessageType)

	b, err = keepAliveMessage{}.encode()
	if err != nil {
		t.Fatalf("error encoding keepalive message: %v", err)
	}
	if _, err = h.peerConn.Write(b); err != nil {
		t.Fatalf("error writing keepalive message: %v", err)
	}

	select {
	case writer := <-h.plugin.establishedCh:
		return writer
	case <-time.After(teardownTimeout):
		t.Fatal("timed out waiting for the session to be established")
		return nil
	}
}

// wedgeUpdate blocks a plugin write in the connection's write lock and returns
// the channel on which that write's result arrives.
func (h *teardownHarness) wedgeUpdate(t *testing.T,
	writer UpdateMessageWriter) chan error {
	t.Helper()

	errCh := make(chan error, 1)
	go func() {
		errCh <- writer.WriteUpdate(make([]byte, 16))
	}()
	select {
	case err := <-errCh:
		t.Fatalf("update write completed against a peer that is not reading: %v", err)
		return nil
	case <-time.After(wedgeSettleTime):
		return errCh
	}
}

// TestDeletePeerWithWedgedWrite covers a peer that has stopped reading with a
// plugin write outstanding. The FSM's own writes queue behind that write, so
// only closing the connection lets the FSM reach its teardown.
func TestDeletePeerWithWedgedWrite(t *testing.T) {
	h := newTeardownHarness(t)
	writer := h.establish(t)
	updateErrCh := h.wedgeUpdate(t, writer)

	deletedCh := make(chan error, 1)
	go func() {
		deletedCh <- h.server.DeletePeer(h.peerAddr)
	}()

	select {
	case err := <-deletedCh:
		if err != nil {
			t.Fatalf("error deleting peer: %v", err)
		}
	case <-time.After(teardownTimeout):
		t.Fatalf("DeletePeer did not return within %s", teardownTimeout)
	}

	select {
	case err := <-updateErrCh:
		if err == nil {
			t.Fatal("wedged update write returned a nil error")
		}
	case <-time.After(teardownTimeout):
		t.Fatal("wedged update write did not return")
	}

	select {
	case <-h.plugin.closedCh:
	case <-time.After(teardownTimeout):
		t.Fatal("OnClose did not fire")
	}
}

// TestDeletePeerSendsCeaseToReadingPeer covers a peer that is still reading.
// The grace period leaves the CEASE to the FSM, so an administrative teardown
// remains graceful.
func TestDeletePeerSendsCeaseToReadingPeer(t *testing.T) {
	h := newTeardownHarness(t)
	h.establish(t)

	type readResult struct {
		msgType uint8
		body    []byte
		err     error
	}
	readCh := make(chan readResult, 1)
	go func() {
		msgType, body, err := h.readMessage()
		readCh <- readResult{msgType: msgType, body: body, err: err}
	}()

	deletedCh := make(chan error, 1)
	go func() {
		deletedCh <- h.server.DeletePeer(h.peerAddr)
	}()

	select {
	case res := <-readCh:
		if res.err != nil {
			t.Fatalf("error reading cease notification: %v", res.err)
		}
		if res.msgType != notificationMessageType {
			t.Fatalf("read message of type %d, want %d", res.msgType,
				notificationMessageType)
		}
		if len(res.body) < 2 {
			t.Fatalf("notification body too short: %d bytes", len(res.body))
		}
		if res.body[0] != NOTIF_CODE_CEASE {
			t.Fatalf("notification code is %d, want %d", res.body[0],
				NOTIF_CODE_CEASE)
		}
	case <-time.After(teardownTimeout):
		t.Fatal("no cease notification arrived")
	}

	select {
	case err := <-deletedCh:
		if err != nil {
			t.Fatalf("error deleting peer: %v", err)
		}
	case <-time.After(teardownTimeout):
		t.Fatalf("DeletePeer did not return within %s", teardownTimeout)
	}
}

// TestServerCloseWithWedgedWrite covers the same teardown reached through
// Server.Close() rather than DeletePeer.
func TestServerCloseWithWedgedWrite(t *testing.T) {
	h := newTeardownHarness(t)
	writer := h.establish(t)
	h.wedgeUpdate(t, writer)

	closedCh := make(chan struct{})
	go func() {
		defer close(closedCh)
		h.server.Close()
	}()

	select {
	case <-closedCh:
	case <-time.After(teardownTimeout):
		t.Fatalf("Server.Close did not return within %s", teardownTimeout)
	}

	select {
	case err := <-h.serveCh:
		if !errors.Is(err, ErrServerClosed) {
			t.Fatalf("Serve returned %v, want %v", err, ErrServerClosed)
		}
	case <-time.After(teardownTimeout):
		t.Fatal("Serve did not return")
	}
}
