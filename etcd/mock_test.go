package etcd

import (
	"reflect"
	"testing"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/check"
	"ergo.services/ergo/testing/mock"
)

// newMockNode returns a gen.NodeRegistrar backed by the ergo mock harness.
func newMockNode(t *testing.T, name string) *mock.Node {
	node := mock.NewNodeT(t)
	node.OnName(func() gen.Atom { return gen.Atom(name) })
	return node
}

// eventMessage narrows a recorded SendEvent to one published message.
func eventMessage(want any) func(check.SendEvent) bool {
	return func(r check.SendEvent) bool { return reflect.DeepEqual(r.Message, want) }
}

// eventType narrows a recorded SendEvent to any message of type E.
func eventType[E any]() func(check.SendEvent) bool {
	return func(r check.SendEvent) bool {
		_, ok := r.Message.(E)
		return ok
	}
}

// registerNode creates a registrar for the given options, registers a mock node
// under the given name and returns both. Termination is registered as cleanup.
func registerNode(t *testing.T, options Options, name string) (*client, *mock.Node) {
	t.Helper()

	registrar, err := Create(options)
	if err != nil {
		t.Fatalf("failed to create registrar: %v", err)
	}
	t.Cleanup(registrar.Terminate)

	c := registrar.(*client)
	node := newMockNode(t, name)

	if _, err := c.Register(node, gen.RegisterRoutes{
		Routes: []gen.Route{{Host: "localhost", Port: 9001, TLS: false}},
	}); err != nil {
		t.Fatalf("failed to register %s: %v", name, err)
	}

	return c, node
}
