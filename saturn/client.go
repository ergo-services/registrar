package saturn

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
	"ergo.services/ergo/net/edf"
)

const (
	handleStateSleep      int32 = 0
	handleStateRunning    int32 = 1
	handleStateTerminated int32 = 2
)

type Options struct {
	Cluster            string
	Port               uint16
	KeepAlive          time.Duration
	InsecureSkipVerify bool
}

func Create(host string, token string, options Options) gen.Registrar {
	if options.Port == 0 {
		options.Port = defaultRegistrarPort
	}
	if options.KeepAlive == 0 {
		options.KeepAlive = defaultKeepAlive
	}

	tlsdialer := &tls.Dialer{
		NetDialer: &net.Dialer{
			KeepAlive: options.KeepAlive,
			Timeout:   defaultConnectTimeout,
		},
		Config: &tls.Config{
			InsecureSkipVerify: options.InsecureSkipVerify,
		},
	}

	return &client{
		dsn:     net.JoinHostPort(host, strconv.Itoa(int(options.Port))),
		dialer:  tlsdialer,
		token:   token,
		cluster: options.Cluster,

		config: make(map[string]any),
		nodes:  make(map[gen.Atom]bool),
		apps:   make(map[gen.Atom][]gen.ApplicationRoute),
		queue:  lib.NewQueueMPSC(),
	}
}

type client struct {
	sync.RWMutex

	dsn     string
	dialer  *tls.Dialer
	token   string
	cluster string

	node gen.NodeRegistrar

	eventRef gen.Ref
	event    gen.Event

	routes gen.RegisterRoutes

	conn  net.Conn
	chunk []byte

	config map[string]any
	apps   map[gen.Atom][]gen.ApplicationRoute
	nodes  map[gen.Atom]bool

	requests sync.Map // ID (uint32) => chan any
	queue    lib.QueueMPSC
	// request handler state
	state int32 // 0 - sleep , 2 - running, 3 - terminated

	id uint32
}

//
// gen.Resolver interface implementation
//

func (c *client) Resolve(name gen.Atom) ([]gen.Route, error) {
	if c.isHandlerTerminated() {
		return nil, gen.ErrRegistrarTerminated
	}

	if name == c.node.Name() {
		return nil, gen.ErrNotAllowed
	}

	id := atomic.AddUint32(&c.id, 1)

	req := MessageResolve{
		ID:   id,
		Name: name,
	}

	v, err := c.request(id, req)
	if err != nil {
		return nil, err
	}

	response, ok := v.(MessageResolveResult)
	if ok == false {
		c.node.Log().Error("(saturn) received incorrect resolve result: %#v\n", v)
		return nil, gen.ErrMalformed
	}

	if response.Error != nil {
		return nil, response.Error
	}

	return response.Routes, nil
}

func (c *client) ResolveApplication(name gen.Atom) ([]gen.ApplicationRoute, error) {
	if c.isHandlerTerminated() {
		return nil, gen.ErrRegistrarTerminated
	}

	c.RLock()
	defer c.RUnlock()

	if ar, found := c.apps[name]; found {
		arcopy := make([]gen.ApplicationRoute, len(ar))
		copy(arcopy, ar)
		return arcopy, nil
	}

	return nil, gen.ErrNoRoute
}

func (c *client) ResolveProxy(name gen.Atom) ([]gen.ProxyRoute, error) {
	if c.isHandlerTerminated() {
		return nil, gen.ErrRegistrarTerminated
	}

	if name == c.node.Name() {
		return nil, gen.ErrNotAllowed
	}

	id := atomic.AddUint32(&c.id, 1)

	req := MessageResolveProxy{
		ID:   id,
		Name: name,
	}

	v, err := c.request(id, req)
	if err != nil {
		return nil, err
	}

	response, ok := v.(MessageResolveProxyResult)
	if ok == false {
		c.node.Log().Error("(saturn) received incorrect resolve proxy result: %#v\n", v)
		return nil, gen.ErrMalformed
	}

	if response.Error != nil {
		return nil, response.Error
	}

	return response.Routes, nil
}

//
// gen.Registrar interface implementation
//

func (c *client) Resolver() gen.Resolver {
	return c
}

func (c *client) RegisterProxy(to gen.Atom) error {
	// TODO will be implemented later
	return gen.ErrUnsupported
}
func (c *client) UnregisterProxy(to gen.Atom) error {
	// TODO will be implemented later
	return gen.ErrUnsupported
}

func (c *client) RegisterApplicationRoute(route gen.ApplicationRoute) error {
	if c.isHandlerTerminated() {
		return gen.ErrRegistrarTerminated
	}

	c.node.Log().Trace("(saturn) register application route for %s", route.Name)

	mra := MessageRegisterApplicationRoute{
		Route: route,
	}
	c.queue.Push(mra)
	c.handle()
	return nil
}
func (c *client) UnregisterApplicationRoute(name gen.Atom) error {
	if c.isHandlerTerminated() {
		return gen.ErrRegistrarTerminated
	}

	c.node.Log().Trace("(saturn) unregister application route for %s", name)

	mua := MessageUnregisterApplicationRoute{
		Name: name,
	}
	c.queue.Push(mua)
	c.handle()
	return nil
}

func (c *client) Nodes() ([]gen.Atom, error) {
	var nodes []gen.Atom

	if c.isHandlerTerminated() {
		return nil, gen.ErrRegistrarTerminated
	}

	c.RLock()
	defer c.RUnlock()

	for n := range c.nodes {
		nodes = append(nodes, n)
	}
	return nodes, nil
}

func (c *client) ConfigItem(item string) (any, error) {
	if c.isHandlerTerminated() {
		return nil, gen.ErrRegistrarTerminated
	}

	nodename := string(c.node.Name())

	c.RLock()
	defer c.RUnlock()

	itm := fmt.Sprintf("%s:%s:%s", c.cluster, nodename, item)
	if v, found := c.config[itm]; found {
		return v, nil
	}

	// cluster:*:item -> value
	itm = fmt.Sprintf("%s:*:%s", c.cluster, item)
	if v, found := c.config[itm]; found {
		return v, nil
	}
	// nodename:item -> value
	itm = fmt.Sprintf("%s:%s", nodename, item)
	if v, found := c.config[itm]; found {
		return v, nil
	}
	// *:item -> value
	itm = fmt.Sprintf("*:%s", item)
	if v, found := c.config[itm]; found {
		return v, nil
	}

	return nil, gen.ErrUnknown
}

func (c *client) Config(items ...string) (map[string]any, error) {
	if c.isHandlerTerminated() {
		return nil, gen.ErrRegistrarTerminated
	}

	config := make(map[string]any)
	nodename := string(c.node.Name())

	c.RLock()
	defer c.RUnlock()

	for _, item := range items {
		// cluster:nodename:item -> value
		itm := fmt.Sprintf("%s:%s:%s", c.cluster, nodename, item)
		if v, found := c.config[itm]; found {
			config[item] = v
			continue
		}

		// cluster:*:item -> value
		itm = fmt.Sprintf("%s:*:%s", c.cluster, item)
		if v, found := c.config[itm]; found {
			config[item] = v
			continue
		}
		// nodename:item -> value
		itm = fmt.Sprintf("%s:%s", nodename, item)
		if v, found := c.config[itm]; found {
			config[item] = v
			continue
		}
		// *:item -> value
		itm = fmt.Sprintf("*:%s", item)
		if v, found := c.config[itm]; found {
			config[item] = v
		}

		// not found
	}

	return config, nil
}
func (c *client) Event() (gen.Event, error) {
	var empty gen.Event

	if c.isHandlerTerminated() {
		return empty, gen.ErrRegistrarTerminated
	}

	return c.event, nil
}
func (c *client) Info() gen.RegistrarInfo {
	var server string
	if c.conn != nil {
		server = c.conn.RemoteAddr().String()
	}
	return gen.RegistrarInfo{
		Server:                     server,
		EmbeddedServer:             false,
		Version:                    c.Version(),
		SupportConfig:              true,
		SupportEvent:               true,
		SupportRegisterProxy:       true,
		SupportRegisterApplication: true,
	}
}

//
// gen.Registrar interface implementation
//

func (c *client) Register(node gen.NodeRegistrar, routes gen.RegisterRoutes) (gen.StaticRoutes, error) {
	c.node = node
	c.routes = routes

	conn, err := c.dialer.Dial("tcp", c.dsn)
	if err != nil {
		return gen.StaticRoutes{}, err
	}

	c.node.Log().Trace("(saturn) trying to handshake with the registrar")
	if err := c.tryHandshake(conn); err != nil {
		conn.Close()
		return gen.StaticRoutes{}, err
	}
	c.node.Log().Trace("(saturn) handshaked successfully")

	c.node.Log().Trace("(saturn) trying to register node on the registrar")
	static, err := c.tryRegister(conn)
	if err != nil {
		return gen.StaticRoutes{}, err
	}

	c.node.Log().Trace("(saturn) registered node %s on the registrar", node.Name())

	eventName := gen.Atom(fmt.Sprintf("saturn.event.%s", conn.RemoteAddr()))
	ref, err := node.RegisterEvent(eventName, gen.EventOptions{})
	if err != nil {
		conn.Close()
		return gen.StaticRoutes{}, err
	}
	c.eventRef = ref
	c.event = gen.Event{Name: eventName, Node: node.Name()}
	c.conn = conn
	c.node.Log().Trace("(saturn) registered local event %s", c.event)

	go c.serve(conn) // reader

	return static, nil
}

func (c *client) Terminate() {
	atomic.StoreInt32(&c.state, handleStateTerminated)
	if c.conn != nil {
		c.conn.Close()
	}

	c.node.UnregisterEvent(c.event.Name)
	c.node.Log().Trace("(saturn) registrar client terminated")
}

func (c *client) Version() gen.Version {
	return gen.Version{
		Name:    clientName,
		Release: clientRelease,
		License: gen.LicenseBSL1,
	}
}

// internals

func (c *client) tryHandshake(conn net.Conn) error {
	salt := lib.RandomString(64)
	hash := sha256.New()
	hash.Write([]byte(fmt.Sprintf("%s:%s", salt, c.token)))
	digest := fmt.Sprintf("%x", hash.Sum(nil))

	handshake := MessageHandshake{
		Salt:   salt,
		Digest: digest,
	}

	if err := c.writeMessage(conn, handshake); err != nil {
		return err
	}

	v, err := c.readMessage(conn, time.Second)
	if err != nil {
		return err
	}

	result, ok := v.(MessageHandshakeResult)
	if ok == false {
		c.node.Log().Error("(saturn) incorrect handshake result: %#v", v)
		return gen.ErrMalformed
	}

	hash.Reset()
	hash.Write([]byte(fmt.Sprintf("%s:%s", handshake.Digest, c.token)))
	if result.Digest != fmt.Sprintf("%x", hash.Sum(nil)) {
		c.node.Log().Error("(saturn) incorrect handshake digest (bad actor)")
		return gen.ErrMalformed
	}

	return nil
}

func (c *client) tryRegister(conn net.Conn) (gen.StaticRoutes, error) {
	var static gen.StaticRoutes

	register := MessageRegister{
		Cluster:        c.cluster,
		Node:           c.node.Name(),
		RegisterRoutes: c.routes,
	}

	if err := c.writeMessage(conn, register); err != nil {
		return static, err
	}

	v, err := c.readMessage(conn, time.Second)
	if err != nil {
		return static, err
	}

	result, ok := v.(MessageRegisterResult)
	if ok == false {
		c.node.Log().Error("(saturn) incorrect register result: %#v", v)
		return static, gen.ErrMalformed
	}

	if result.Error != nil {
		return static, result.Error
	}

	c.config = result.Config
	for _, node := range result.Nodes {
		c.nodes[node] = true
	}
	for _, appRoute := range result.Applications {
		ar := c.apps[appRoute.Name]
		ar = append(ar, appRoute)
		c.apps[appRoute.Name] = ar
	}

	return static, nil
}

func (c *client) writeMessage(conn net.Conn, message any) error {
	buf := lib.TakeBuffer()
	defer lib.ReleaseBuffer(buf)

	buf.Allocate(4)
	buf.B[0] = Proto
	buf.B[1] = ProtoVersion

	if err := edf.Encode(message, buf, edf.Options{}); err != nil {
		return err
	}

	binary.BigEndian.PutUint16(buf.B[2:4], uint16(buf.Len()-4))
	l := buf.Len()
	lenP := l
	for {
		n, e := conn.Write(buf.B[lenP-l:])
		if e != nil {
			return e
		}
		// check if something left
		l -= n
		if l == 0 {
			break
		}
	}
	return nil
}

func (c *client) readMessage(conn net.Conn, timeout time.Duration) (any, error) {
	var b [1024]byte

	buf := append(c.chunk, b[:]...)
	c.chunk = []byte{}
	if timeout == 0 {
		conn.SetReadDeadline(time.Time{})
	}

	for {

		if timeout > 0 {
			conn.SetReadDeadline(time.Now().Add(timeout))
		}

		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}

		c.chunk = append(c.chunk, buf[:n]...)
		if len(c.chunk) < 4 {
			continue
		}

		if c.chunk[0] != Proto {
			c.node.Log().Error("(saturn) received malfomed message from the registrar")
			return nil, gen.ErrMalformed
		}
		if c.chunk[1] != ProtoVersion {
			c.node.Log().Error("(saturn) received mismatch proto version from the registrar")
			return nil, gen.ErrMalformed
		}

		l := int(binary.BigEndian.Uint16(c.chunk[2:4]))
		if 4+l > len(c.chunk) {
			continue
		}

		v, ch, err := edf.Decode(c.chunk[4:], edf.Options{})
		if err != nil {
			c.node.Log().Error("(saturn) unable to decode reply message from the registrar: %s", err)
			return nil, gen.ErrMalformed
		}
		c.chunk = ch

		return v, nil
	}

}

func (c *client) serve(conn net.Conn) {
	for {
		message, err := c.readMessage(conn, 0)

		if c.isHandlerTerminated() {
			return
		}

		if err == nil {
			switch m := message.(type) {
			case MessageResolveResult:
				v, found := c.requests.Load(m.ID)
				if found == false {
					// unknown ID (maybe too late)
					break
				}
				c.node.Log().Trace("(saturn) received resolve result: %#v\n", m)
				ch := v.(chan any)
				select {
				case ch <- m:
				default:
					c.node.Log().Trace("(saturn) unable to deliver resolve result")
				}

			case MessageResolveProxyResult:
				v, found := c.requests.Load(m.ID)
				if found == false {
					// unknown ID (maybe too late)
					break
				}
				c.node.Log().Trace("(saturn) received resolve proxy result: %#v\n", m)
				ch := v.(chan any)
				select {
				case ch <- m:
				default:
					c.node.Log().Trace("(saturn) unable to deliver resolve proxy result")
				}

			case MessageConfigUpdate:
				nodename := string(c.node.Name())

				c.Lock()
				c.config[m.Item] = m.Value
				c.Unlock()

				c.node.Log().Trace("(saturn) received config update for %q", m.Item)

				// cluster:nodename:item -> value
				itm := fmt.Sprintf("%s:%s:", c.cluster, nodename)
				if strings.HasPrefix(m.Item, itm) {
					itemName := strings.TrimPrefix(m.Item, itm)
					ev := EventConfigUpdate{
						Item:  itemName,
						Value: m.Value,
					}
					if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
						c.node.Log().Error("(saturn) unable to send event with config update: %s", err)
					}
					break
				}

				// cluster:*:item -> value
				itm = fmt.Sprintf("%s:*:", c.cluster)
				if strings.HasPrefix(m.Item, itm) {
					itemName := strings.TrimPrefix(m.Item, itm)
					itm1 := fmt.Sprintf("%s:%s:%s", c.cluster, nodename, itemName)

					c.RLock()
					if _, found := c.config[itm1]; found {
						// we have more precise config value for this item name
						c.RUnlock()
						break
					}
					c.RUnlock()

					ev := EventConfigUpdate{
						Item:  itemName,
						Value: m.Value,
					}
					c.node.Log().Trace("(saturn) config update has new value for %q. send event", m.Item)
					if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
						c.node.Log().Error("(saturn) unable to send event with config update: %s", err)
					}
					break
				}

				// nodename:item -> value
				itm = fmt.Sprintf("%s:", nodename)
				if strings.HasPrefix(m.Item, itm) {
					itemName := strings.TrimPrefix(m.Item, itm)
					itm1 := fmt.Sprintf("%s:%s:%s", c.cluster, nodename, itemName)

					c.RLock()
					if _, found := c.config[itm1]; found {
						// we have more precise config value for this item name
						c.RUnlock()
						break
					}
					itm1 = fmt.Sprintf("%s:*:%s", c.cluster, itemName)
					if _, found := c.config[itm1]; found {
						// we have more precise config value for this item name
						c.RUnlock()
						break
					}
					c.RUnlock()

					ev := EventConfigUpdate{
						Item:  itemName,
						Value: m.Value,
					}
					c.node.Log().Trace("(saturn) config update has new value for %q. send event", m.Item)
					if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
						c.node.Log().Error("(saturn) unable to send event with config update: %s", err)
					}
					break
				}

				// *:item -> value
				if strings.HasPrefix(m.Item, "*:") {
					itemName := strings.TrimPrefix(m.Item, itm)
					itm1 := fmt.Sprintf("%s:%s:%s", c.cluster, nodename, itemName)

					c.RLock()
					if _, found := c.config[itm1]; found {
						// we have more precise config value for this item name
						c.RUnlock()
						break
					}
					itm1 = fmt.Sprintf("%s:*:%s", c.cluster, itemName)
					if _, found := c.config[itm1]; found {
						// we have more precise config value for this item name
						c.RUnlock()
						break
					}
					itm1 = fmt.Sprintf("%s:%s", nodename, itemName)
					if _, found := c.config[itm1]; found {
						// we have more precise config value for this item name
						c.RUnlock()
						break
					}
					c.RUnlock()

					ev := EventConfigUpdate{
						Item:  itemName,
						Value: m.Value,
					}
					c.node.Log().Trace("(saturn) config update has new value for %q. send event", m.Item)
					if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
						c.node.Log().Error("(saturn) unable to send event with config update: %s", err)
					}
					break
				}

			case MessageNodeJoined:
				c.Lock()
				c.node.Log().Trace("(saturn) remote node %s joined the cluster %q", m.Node, c.cluster)
				c.nodes[m.Node] = true
				c.Unlock()
				ev := EventNodeJoined{
					Name: m.Node,
				}
				if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
					c.node.Log().Error("(saturn) unable to send event with cluster node update: %s", err)
				}

			case MessageNodeLeft:
				c.node.Log().Trace("(saturn) remote node %s left the cluster %q", m.Node, c.cluster)
				c.Lock()
				delete(c.nodes, m.Node)

				// remove app routes for this node
				for app, routes := range c.apps {
					for i := range routes {
						if routes[i].Node != m.Node {
							continue
						}
						c.node.Log().Trace("(saturn) removed application route for %s (remote node %s left the cluster %q)", app, m.Node, c.cluster)
						routes[0] = routes[i]
						routes = routes[1:]

						if len(routes) == 0 {
							delete(c.apps, app)
						} else {
							c.apps[app] = routes
						}

						break
					}
				}
				c.Unlock()

				ev := EventNodeLeft{
					Name: m.Node,
				}
				if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
					c.node.Log().Error("(saturn) unable to send event with cluster node update: %s", err)
				}

			case MessageRegisterApplicationRoute:
				c.Lock()

				routes, found := c.apps[m.Route.Name]

				for i := range routes {
					if routes[i].Node != m.Route.Node {
						continue
					}
					found = true
					routes[i] = m.Route
					c.node.Log().Trace("(saturn) updated application route for %s (application running on %s)", m.Route.Name, m.Route.Node)
					break
				}

				if found == false {
					routes = append(routes, m.Route)
					c.apps[m.Route.Name] = routes
					c.node.Log().Trace("(saturn) added application route for %s (application started on %s)", m.Route.Name, m.Route.Node)
				}
				c.Unlock()

				switch m.Route.State {
				case gen.ApplicationStateLoaded:
					if found == false {
						ev := EventApplicationLoaded{
							Name:   m.Route.Name,
							Node:   m.Route.Node,
							Weight: m.Route.Weight,
						}
						c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev)
						break
					}
					ev := EventApplicationStopped{
						Name: m.Route.Name,
						Node: m.Route.Node,
					}
					c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev)
				case gen.ApplicationStateStopping:
					ev := EventApplicationStopping{
						Name: m.Route.Name,
						Node: m.Route.Node,
					}
					c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev)
				case gen.ApplicationStateRunning:
					ev := EventApplicationStarted{
						Name:   m.Route.Name,
						Node:   m.Route.Node,
						Weight: m.Route.Weight,
						Mode:   m.Route.Mode,
					}
					c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev)
				}

			case MessageUnregisterApplicationRoute:
				c.Lock()
				routes, found := c.apps[m.Name]
				for i := range routes {
					if routes[i].Node != m.Node {
						continue
					}
					c.node.Log().Trace("(saturn) remove application route for %s (application stopped on %s )",
						m.Name, m.Node)
					routes[0] = routes[i]
					routes = routes[1:]
					found = true

					if len(routes) == 0 {
						delete(c.apps, m.Name)
					} else {
						c.apps[m.Name] = routes
					}

					break
				}
				c.Unlock()

				if found == false {
					break
				}
				ev := EventApplicationUnloaded{
					Name: m.Name,
					Node: m.Node,
				}
				c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev)

			default:
				c.node.Log().Error("(saturn) unknown message from registrar: %#v", m)
			}
			continue
		}

		if err != io.EOF {
			if err != gen.ErrMalformed {
				continue
			}
			c.conn.Close()
			c.node.Log().Warning("(saturn) drop connection")
		} else {
			// disconnected
			c.node.Log().Warning("(saturn) lost connection with the registrar, trying to reconnect...")
		}

		// clean it up by making the new ones
		c.nodes = make(map[gen.Atom]bool)
		c.apps = make(map[gen.Atom][]gen.ApplicationRoute)
		c.conn = nil

		// trying to reconnect
		reconnectTimeout := time.Second
		for {
			if c.isHandlerTerminated() {
				return
			}
			reconn, err := c.dialer.Dial("tcp", c.dsn)
			if err != nil {
				c.node.Log().Error("unable to connect to the registrar: %s", err)
				time.Sleep(reconnectTimeout)
				if reconnectTimeout < 5*time.Second {
					reconnectTimeout += time.Second
				}
				continue
			}
			c.node.Log().Trace("(saturn) trying to re-handshake with the registrar")
			if err := c.tryHandshake(reconn); err != nil {
				c.node.Log().Error("(saturn) unable to re-handshake node on the registrar: %s", err)
				reconn.Close()
				time.Sleep(reconnectTimeout)
				if reconnectTimeout < 5*time.Second {
					reconnectTimeout += time.Second
				}
				continue
			}
			c.node.Log().Trace("(saturn) re-handshaked successfully")

			c.node.Log().Trace("(saturn) trying to re-register node on the registrar")
			if _, err := c.tryRegister(reconn); err != nil {
				c.node.Log().Error("(saturn) unable to re-register node on the registrar: %s", err)
				reconn.Close()
				time.Sleep(reconnectTimeout)
				if reconnectTimeout < 5*time.Second {
					reconnectTimeout += time.Second
				}
				continue
			}

			c.conn = reconn
			conn = reconn

			c.node.Log().Info("(saturn) re-registered node on the registrar")

			c.handle() // handle the queue

			break
		}

	}
}

func (c *client) handle() {
	if atomic.CompareAndSwapInt32(&c.state, handleStateSleep, handleStateRunning) == false {
		// running or terminated
		return
	}

	go func() {
		var handled int

		c.node.Log().Trace("(saturn) starting queue handler")
		defer func() { c.node.Log().Trace("(saturn) stopped queue handler (sent messages: %d)", handled) }()

	next:
		for {
			if atomic.LoadInt32(&c.state) != handleStateRunning {
				// terminated
				break
			}
			msg, ok := c.queue.Pop()
			if ok == false {
				// no messages
				break
			}

			if err := c.writeMessage(c.conn, msg); err != nil {
				c.node.Log().Error("(saturn) unable to send message to the registrar: %s", err)
				continue
			}
			handled++

		}

		if atomic.CompareAndSwapInt32(&c.state, handleStateRunning, handleStateSleep) == false {
			// running or terminated
			return
		}

		if c.queue.Item() == nil {
			return
		}

		if atomic.CompareAndSwapInt32(&c.state, handleStateSleep, handleStateRunning) == false {
			// another goroutine is already started
		}
		goto next
	}()

}

func (c *client) isHandlerTerminated() bool {
	return atomic.LoadInt32(&c.state) == handleStateTerminated
}

func (c *client) request(id uint32, r any) (any, error) {
	ch := make(chan any)

	c.requests.Store(id, ch)
	defer c.requests.Delete(id)

	timer := lib.TakeTimer()
	defer lib.ReleaseTimer(timer)

	c.queue.Push(r)
	c.handle()

	timer.Reset(time.Second * 3)
	select {
	case v := <-ch:
		return v, nil

	case <-timer.C:
		return nil, gen.ErrTimeout
	}
}
