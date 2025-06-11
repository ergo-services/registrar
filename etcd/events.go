package etcd

import (
	"ergo.services/ergo/gen"
)

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
