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
	// Fast path: serve from cache if populated (rev > 0 means initial Get
	// completed and the entry is tracking Watch updates).
	c.appCacheLock.RLock()
	if entry, ok := c.appCache[name]; ok && entry.rev > 0 {
		routes := c.filterAppRoutes(entry)
		c.appCacheLock.RUnlock()
		if len(routes) == 0 {
			return nil, gen.ErrNoRoute
		}
		return routes, nil
	}
	c.appCacheLock.RUnlock()

	// Cache miss. Install a placeholder BEFORE issuing Get so that any
	// Watch event arriving during the Get has a target entry to update.
	// Otherwise such events would be dropped and the Get could overwrite
	// a newer state with stale data.
	c.appCacheLock.Lock()
	entry, ok := c.appCache[name]
	if ok == false {
		entry = &appEntry{routes: make(map[gen.Atom]gen.ApplicationRoute)}
		c.appCache[name] = entry
	}
	c.appCacheLock.Unlock()

	key := c.pathApps + string(name)
	ctx, cancel := context.WithTimeout(context.Background(), c.options.RequestTimeout)
	defer cancel()

	resp, err := c.cli.Get(ctx, key, etcdcli.WithPrefix())
	if err != nil {
		// Drop placeholder so a subsequent resolve can retry the Get.
		// Only drop if still unpopulated — a concurrent Watch event may
		// have legitimately filled the entry while Get was in flight.
		c.appCacheLock.Lock()
		if cur, ok := c.appCache[name]; ok && cur == entry && cur.rev == 0 {
			delete(c.appCache, name)
		}
		c.appCacheLock.Unlock()
		return nil, err
	}

	getRev := resp.Header.Revision

	c.appCacheLock.Lock()
	if entry.rev < getRev {
		// No newer Watch event has touched this entry — Get result wins.
		routes := make(map[gen.Atom]gen.ApplicationRoute, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			v, decErr := decode(kv.Value)
			if decErr != nil {
				c.node.Log().Error("(registrar) failed to decode application route for %s: %v", kv.Key, decErr)
				continue
			}
			route, rok := v.(gen.ApplicationRoute)
			if rok == false {
				c.node.Log().Error("(registrar) invalid type for application route %s: %T", kv.Key, v)
				continue
			}
			routes[route.Node] = route
		}
		entry.routes = routes
		entry.rev = getRev
	}
	// else: a Watch event with rev >= getRev already updated the entry;
	// Get result is stale relative to that event — keep the cache as is.
	result := c.filterAppRoutes(entry)
	c.appCacheLock.Unlock()

	if len(result) == 0 {
		return nil, gen.ErrNoRoute
	}
	return result, nil
}

// filterAppRoutes copies cache entry routes excluding the self node.
// Caller must hold appCacheLock (read or write).
func (c *client) filterAppRoutes(entry *appEntry) []gen.ApplicationRoute {
	selfName := c.node.Name()
	routes := make([]gen.ApplicationRoute, 0, len(entry.routes))
	for nodeName, route := range entry.routes {
		if nodeName == selfName {
			continue
		}
		routes = append(routes, route)
	}
	return routes
}

func (c *client) ResolveProxy(name gen.Atom) ([]gen.ProxyRoute, error) {
	// Proxy routing is not supported in etcd registrar implementation
	return nil, gen.ErrNoRoute
}
