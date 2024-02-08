package saturn

import (
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/edf"
)

const (
	defaultRegistrarPort  uint16        = 4499
	defaultKeepAlive      time.Duration = 3 * time.Second
	defaultConnectTimeout time.Duration = 3 * time.Second

	Proto        byte = 99
	ProtoVersion byte = 1
)

type MessageHandshake struct {
	Salt string // random string
	// Digest
	// hash := sha256.New()
	// hash.Write([]byte(fmt.Sprintf("%s:%s", Salt, token)))
	// digest := fmt.Sprintf("%x", hash.Sum(nil))
	Digest string
}

type MessageHandshakeResult struct {
	// Digest (against the MITM attack. the registrar must prove to have the same token)
	// hash := sha256.New()
	// hash.Write([]byte(fmt.Sprintf("%s:%s", MessageHandshake.Digest, token)))
	// digest := fmt.Sprintf("%x", hash.Sum(nil))
	Digest string
}

type MessageRegister struct {
	Cluster        string
	Hidden         bool
	Node           gen.Atom
	RegisterRoutes gen.RegisterRoutes
}

type MessageRegisterResult struct {
	Error        error
	Config       map[string]any
	Nodes        []gen.Atom
	Applications []gen.ApplicationRoute
}

type MessageRegisterProxy struct {
	Route gen.ProxyRoute
}

type MessageUnregisterProxy struct {
	Route gen.ProxyRoute
}

type MessageRegisterApplication struct {
	Route gen.ApplicationRoute
}

type MessageUnregisterApplication struct {
	Name   gen.Atom
	Reason error
}

type MessageResolve struct {
	ID   uint32
	Name gen.Atom
}

type MessageResolveResult struct {
	ID     uint32
	Error  error
	Routes []gen.Route
}

type MessageResolveProxy struct {
	ID   uint32
	Name gen.Atom
}

type MessageResolveProxyResult struct {
	ID     uint32
	Error  error
	Routes []gen.ProxyRoute
}

type MessageConfigUpdate struct {
	Item  string
	Value any
}

type MessageApplicationStarted struct {
	Route gen.ApplicationRoute
}

type MessageApplicationTerminated struct {
	Name   gen.Atom
	Node   gen.Atom
	Reason error
}

type MessageNodeJoined struct {
	Node gen.Atom
}

type MessageNodeLeft struct {
	Node gen.Atom
}

type EventConfigUpdate struct {
	Item  string
	Value any
}

type EventNodeJoined struct {
	Name gen.Atom
}

type EventNodeLeft struct {
	Name gen.Atom
}

func init() {
	types := []any{
		MessageHandshake{},
		MessageHandshakeResult{},
		MessageRegister{},
		MessageRegisterResult{},
		MessageRegisterApplication{},
		MessageResolve{},
		MessageResolveResult{},
		MessageResolveProxy{},
		MessageResolveProxyResult{},
		MessageNodeJoined{},
		MessageNodeLeft{},
		MessageApplicationStarted{},
		MessageApplicationTerminated{},
		MessageConfigUpdate{},
	}

	for _, t := range types {
		err := edf.RegisterTypeOf(t)
		if err == nil || err == gen.ErrTaken {
			continue
		}
		panic(err)
	}
}
