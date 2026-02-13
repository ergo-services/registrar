package etcd

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// TestProxy is a TCP proxy for testing network conditions
// It sits between etcd client and server, allowing programmatic control
// over connection state (block new connections, drop existing ones, etc.)
type TestProxy struct {
	upstream string       // etcd server address (e.g., "localhost:2379")
	listener net.Listener // listen address (e.g., "localhost:5050")

	// Connection control
	blocked atomic.Bool // when true, new connections are rejected
	closed  atomic.Bool // when true, proxy is shutting down

	// Active connections tracking
	mu    sync.Mutex
	conns []net.Conn // all active connections (client and server pairs)

	// Statistics
	acceptedCount atomic.Int64 // total accepted connections
	droppedCount  atomic.Int64 // total dropped connections
	blockedCount  atomic.Int64 // total blocked connection attempts
}

// NewTestProxy creates and starts a new TCP proxy
// listen: address to listen on (e.g., "localhost:5050")
// upstream: etcd server address (e.g., "localhost:2379")
func NewTestProxy(listen, upstream string) (*TestProxy, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	p := &TestProxy{
		upstream: upstream,
		listener: ln,
	}

	// Start accepting connections in background
	go p.acceptLoop()

	return p, nil
}

// acceptLoop continuously accepts new connections until proxy is closed
func (p *TestProxy) acceptLoop() {
	for !p.closed.Load() {
		conn, err := p.listener.Accept()
		if err != nil {
			// Listener closed or error - stop accepting
			return
		}

		// Handle each connection in separate goroutine
		go p.handleConnection(conn)
	}
}

// handleConnection manages a single client connection
func (p *TestProxy) handleConnection(clientConn net.Conn) {
	// Check if proxy is blocking new connections
	if p.blocked.Load() {
		p.blockedCount.Add(1)
		clientConn.Close()
		return
	}

	// Connect to upstream etcd server
	serverConn, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		clientConn.Close()
		return
	}

	p.acceptedCount.Add(1)

	// Track both connections
	p.mu.Lock()
	p.conns = append(p.conns, clientConn, serverConn)
	p.mu.Unlock()

	// Bidirectional proxy: copy data in both directions
	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Server
	go func() {
		defer wg.Done()
		io.Copy(serverConn, clientConn)
		// When client closes, close server connection
		serverConn.Close()
	}()

	// Server -> Client
	go func() {
		defer wg.Done()
		io.Copy(clientConn, serverConn)
		// When server closes, close client connection
		clientConn.Close()
	}()

	// Wait for both directions to complete
	wg.Wait()

	// Remove from tracking
	p.mu.Lock()
	p.removeConns(clientConn, serverConn)
	p.mu.Unlock()
}

// removeConns removes connections from tracking list
func (p *TestProxy) removeConns(conns ...net.Conn) {
	// Create set of connections to remove
	toRemove := make(map[net.Conn]bool)
	for _, c := range conns {
		toRemove[c] = true
	}

	// Filter out removed connections
	filtered := make([]net.Conn, 0, len(p.conns))
	for _, c := range p.conns {
		if !toRemove[c] {
			filtered = append(filtered, c)
		}
	}
	p.conns = filtered
}

// Block prevents new connections from being established
// Existing connections continue to work
func (p *TestProxy) Block() {
	p.blocked.Store(true)
}

// Unblock allows new connections to be established again
func (p *TestProxy) Unblock() {
	p.blocked.Store(false)
}

// IsBlocked returns whether proxy is currently blocking new connections
func (p *TestProxy) IsBlocked() bool {
	return p.blocked.Load()
}

// DropAll closes all active connections immediately
// This simulates a network partition where all connections are severed
func (p *TestProxy) DropAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	count := len(p.conns)
	for _, conn := range p.conns {
		conn.Close()
	}
	p.conns = nil
	p.droppedCount.Add(int64(count))
}

// ActiveConnections returns the number of currently active connections
func (p *TestProxy) ActiveConnections() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns) / 2 // divide by 2 because we track pairs
}

// Stats returns proxy statistics
func (p *TestProxy) Stats() ProxyStats {
	return ProxyStats{
		Accepted: p.acceptedCount.Load(),
		Dropped:  p.droppedCount.Load(),
		Blocked:  p.blockedCount.Load(),
		Active:   int64(p.ActiveConnections()),
	}
}

// Close shuts down the proxy and closes all connections
func (p *TestProxy) Close() error {
	p.closed.Store(true)
	p.DropAll()
	return p.listener.Close()
}

// Addr returns the listen address of the proxy
func (p *TestProxy) Addr() string {
	return p.listener.Addr().String()
}

// ProxyStats contains statistics about proxy operation
type ProxyStats struct {
	Accepted int64 // total accepted connections
	Dropped  int64 // total dropped connections
	Blocked  int64 // total blocked connection attempts
	Active   int64 // currently active connections
}
