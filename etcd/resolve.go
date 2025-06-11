package etcd

import (
	"context"

	"ergo.services/ergo/gen"
	etcdcli "go.etcd.io/etcd/client/v3"
)

//
// gen.Resolver interface implementation
//

func (c *client) Resolve(name gen.Atom) ([]gen.Route, error) {
	// Use exact key lookup for the specific node
	key := c.pathNodes + string(name)

	// Create timeout context for etcd operation
	ctx, cancel := context.WithTimeout(context.Background(), c.options.RequestTimeout)
	defer cancel()

	resp, err := c.cli.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, gen.ErrNoRoute
	}

	v, err := decode(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	routes, ok := v.([]gen.Route)
	if !ok {
		c.node.Log().Error("(registrar) invalid type for node route %s: %T", key, v)
		return nil, gen.ErrNoRoute
	}

	return routes, nil
}

func (c *client) ResolveApplication(name gen.Atom) ([]gen.ApplicationRoute, error) {
	key := c.pathApps + string(name)

	// Create timeout context for etcd operation
	ctx, cancel := context.WithTimeout(context.Background(), c.options.RequestTimeout)
	defer cancel()

	resp, err := c.cli.Get(ctx, key, etcdcli.WithPrefix())
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, gen.ErrNoRoute
	}

	routes := make([]gen.ApplicationRoute, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		v, err := decode(kv.Value)
		if err != nil {
			c.node.Log().Error("(registrar) failed to decode application route for %s: %v", kv.Key, err)
			continue // Skip invalid entries
		}

		route, ok := v.(gen.ApplicationRoute)
		if !ok {
			c.node.Log().Error("(registrar) invalid type for application route %s: %T", kv.Key, v)
			continue
		}

		if route.Node == c.node.Name() {
			continue // skip routes for this node
		}

		routes = append(routes, route)
	}

	return routes, nil
}

func (c *client) ResolveProxy(name gen.Atom) ([]gen.ProxyRoute, error) {
	// Proxy routing is not supported in etcd registrar implementation
	return nil, gen.ErrNoRoute
}
