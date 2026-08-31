package etcd

import (
	"ergo.services/ergo/gen"
)

// Registrar session events. They let a consumer tell "the registrar lost its
// session" from "a node left the cluster", which is the difference between
// waiting and evicting.
type EventRegistrarConnected struct {
	Info gen.RegistrarInfo
}

type EventRegistrarDisconnected struct {
	Reason error
}

// Configuration update events
type EventConfigUpdate struct {
	Item  string
	Value any
}

// Node lifecycle events
type EventNodeJoined struct {
	Name gen.Atom
}

type EventNodeLeft struct {
	Name gen.Atom
}

// Application lifecycle events
type EventApplicationLoaded struct {
	Name   gen.Atom
	Node   gen.Atom
	Weight int
}

type EventApplicationStarted struct {
	Name   gen.Atom
	Node   gen.Atom
	Weight int
	Mode   gen.ApplicationMode
}

type EventApplicationStopping struct {
	Name gen.Atom
	Node gen.Atom
}

type EventApplicationStopped struct {
	Name gen.Atom
	Node gen.Atom
}

type EventApplicationUnloaded struct {
	Name gen.Atom
	Node gen.Atom
}
