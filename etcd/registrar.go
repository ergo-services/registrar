package etcd

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
	etcdcli "go.etcd.io/etcd/client/v3"
)

// gen.Registrar interface implementation
func (c *client) Register(node gen.NodeRegistrar, routes gen.RegisterRoutes) (gen.StaticRoutes, error) {
	c.routes = routes.Routes
	for _, route := range routes.ApplicationRoutes {
		c.apps.Store(route.Name, route)
	}

	c.node = node
	static, err := c.tryRegister()
	if err == nil {
		eventName := gen.Atom(c.pathClusterRoutes)
		eventRef, err := node.RegisterEvent(eventName, gen.EventOptions{})
		if err != nil {
			// Clean up: revoke the lease since keepRegistration won't start
			c.revokeLease(c.leaseID())
			c.setLeaseID(0)
			atomic.StoreInt32(&c.state, 0)
			return gen.StaticRoutes{}, err
		}
		c.event = gen.Event{Name: eventName, Node: node.Name()}
		c.eventRef = eventRef
		go c.keepRegistration()
	}
	return static, err
}

func (c *client) Resolver() gen.Resolver {
	return c
}

func (c *client) RegisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}
func (c *client) UnregisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}

func (c *client) RegisterApplicationRoute(route gen.ApplicationRoute) error {
	c.apps.Store(route.Name, route)
	key := c.pathApps + string(route.Name) + "/" + string(c.node.Name())
	value, err := encode(route)
	if err != nil {
		return err
	}

	// keepRegistration can replace the lease at any moment. A Put that landed on
	// the previous lease is about to be deleted along with it, so make sure the
	// lease is still current and redo the Put otherwise.
	for attempt := 1; ; attempt++ {
		lease := c.leaseID()

		ctx, cancel := c.requestContext()
		_, err := c.cli.Put(ctx, key, value, etcdcli.WithLease(lease))
		cancel()
		if err != nil {
			return err
		}
		if c.leaseID() == lease {
			return nil
		}
		if attempt == 3 {
			// Give up racing. The route stays in c.apps, so the re-registration
			// that replaced the lease will publish it anyway.
			c.node.Log().Debug("(registrar) lease kept changing while registering route %s", route.Name)
			return nil
		}
	}
}
func (c *client) UnregisterApplicationRoute(name gen.Atom) error {
	c.apps.Delete(name)
	key := c.pathApps + string(name) + "/" + string(c.node.Name())

	ctx, cancel := c.requestContext()
	defer cancel()

	if _, err := c.cli.Delete(ctx, key); err != nil {
		return err
	}
	return nil
}

func (c *client) Nodes() ([]gen.Atom, error) {
	ctx, cancel := c.requestContext()
	defer cancel()

	resp, err := c.cli.Get(ctx, c.pathNodes, etcdcli.WithPrefix())
	if err != nil {
		return nil, err
	}
	nodes := make([]gen.Atom, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		// Extract node name from etcd key by removing the path prefix
		nodeName := gen.Atom(strings.TrimPrefix(string(kv.Key), c.pathNodes))
		if nodeName == c.node.Name() {
			continue // skip self
		}
		nodes = append(nodes, nodeName)
	}
	return nodes, nil
}

func (c *client) ConfigItem(item string) (any, error) {
	if atomic.LoadInt32(&c.state) == 2 {
		return nil, gen.ErrRegistrarTerminated
	}

	nodename := string(c.node.Name())

	c.configLock.RLock()
	defer c.configLock.RUnlock()

	// Configuration Storage Strategy:
	// - Cluster-specific configs: services/ergo/cluster/{cluster}/config/
	//   Keys: "node/item", "*/item"
	// - Cross-cluster configs: services/ergo/config/
	//   Keys: "cluster/node/item", "global/item"
	//
	// Note: Only configurations relevant to this node are loaded and stored locally.
	// This includes node-specific configs, wildcard configs, and global configs.
	// Configurations for other specific nodes are filtered out during loading.
	//
	// Hierarchical configuration resolution with priority (highest to lowest):
	// 1. cluster/node/item -> value (cross-cluster specific)
	key := fmt.Sprintf("%s/%s/%s", c.options.Cluster, nodename, item)
	if v, found := c.config[key]; found {
		return v, nil
	}

	// 2. node/item -> value (current cluster, specific node)
	key = fmt.Sprintf("%s/%s", nodename, item)
	if v, found := c.config[key]; found {
		return v, nil
	}

	// 3. */item -> value (current cluster default)
	key = fmt.Sprintf("*/%s", item)
	if v, found := c.config[key]; found {
		return v, nil
	}

	// 4. global/item -> value (global default)
	key = fmt.Sprintf("global/%s", item)
	if v, found := c.config[key]; found {
		return v, nil
	}

	return nil, gen.ErrUnknown
}

func (c *client) Config(items ...string) (map[string]any, error) {
	if atomic.LoadInt32(&c.state) == 2 {
		return nil, gen.ErrRegistrarTerminated
	}

	// Note: Only configurations relevant to this node are stored locally,
	// so all returned values are applicable to the current node.
	config := make(map[string]any)
	nodename := string(c.node.Name())

	c.configLock.RLock()
	defer c.configLock.RUnlock()

	for _, item := range items {
		// Try hierarchical resolution for each item
		var found bool

		// 1. cluster/node/item -> value (highest priority - cross-cluster specific)
		key := fmt.Sprintf("%s/%s/%s", c.options.Cluster, nodename, item)
		if v, exists := c.config[key]; exists {
			config[item] = v
			found = true
			continue
		}

		// 2. node/item -> value (current cluster, specific node)
		key = fmt.Sprintf("%s/%s", nodename, item)
		if v, exists := c.config[key]; exists {
			config[item] = v
			found = true
			continue
		}

		// 3. */item -> value (current cluster default)
		key = fmt.Sprintf("*/%s", item)
		if v, exists := c.config[key]; exists {
			config[item] = v
			found = true
			continue
		}

		// 4. global/item -> value (lowest priority - global default)
		key = fmt.Sprintf("global/%s", item)
		if v, exists := c.config[key]; exists {
			config[item] = v
			found = true
		}

		// If not found, the item won't be in the returned map
		_ = found
	}

	return config, nil
}

func (c *client) Event() (gen.Event, error) {
	return c.event, nil
}

func (c *client) Info() gen.RegistrarInfo {
	return gen.RegistrarInfo{
		Server:                     strings.Join(c.options.Endpoints, ","),
		EmbeddedServer:             false,
		Version:                    c.Version(),
		SupportConfig:              true,
		SupportEvent:               true,
		SupportRegisterProxy:       false,
		SupportRegisterApplication: true,
	}
}

func (c *client) Version() gen.Version {
	return version
}

func (c *client) Terminate() {
	atomic.StoreInt32(&c.state, 2) // set state to terminated
	c.cancel()                     // cancel main context - stops KeepAlive immediately
	if lease := c.leaseID(); lease != 0 {
		// Use timeout since main context is already cancelled
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		c.cli.Revoke(revokeCtx, lease)
		revokeCancel()
	}
	c.cli.Close()
	if c.node != nil {
		c.node.Log().Trace("(etcd) registrar client terminated")
	}
}

// internals

// leaseID returns the lease the node is currently registered with.
func (c *client) leaseID() etcdcli.LeaseID {
	return etcdcli.LeaseID(c.lease.Load())
}

func (c *client) setLeaseID(lease etcdcli.LeaseID) {
	c.lease.Store(int64(lease))
}

// requestContext returns a context for a single etcd RPC. clientv3 calls
// default to grpc.WaitForReady(true), so a call without a deadline blocks for
// as long as etcd is unreachable instead of failing. Derived from the client
// context, so Terminate aborts in-flight calls.
func (c *client) requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.ctx, c.options.RequestTimeout)
}

// revokeLease releases the lease on a best-effort basis. Uses a standalone
// context so cleanup still runs when the client context is already cancelled.
func (c *client) revokeLease(lease etcdcli.LeaseID) {
	if lease == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.options.RequestTimeout)
	defer cancel()
	if _, err := c.cli.Revoke(ctx, lease); err != nil {
		if c.node != nil {
			c.node.Log().Debug("(registrar) unable to revoke lease %d: %s", lease, err)
		}
	}
}

func (c *client) keepRegistration() {
	initialLease := c.leaseID()

	for iteration := 0; ; iteration++ {
		// New context for each iteration - derives from c.ctx (cancelled by Terminate)
		iterCtx, iterCancel := context.WithCancel(c.ctx)

		var currentLease etcdcli.LeaseID

		if iteration == 0 {
			// First iteration: use lease from Register()
			currentLease = initialLease
			c.node.Log().Info("(registrar) starting with initial lease %d", currentLease)
		} else {
			// Reconnection: create new registration
			c.node.Log().Info("(registrar) attempting to re-register (iteration %d)", iteration)

			oldLease := c.leaseID()

			// Retry loop with exponential backoff
			for attempt := 1; ; attempt++ {
				err := c.tryReRegister(oldLease)
				if err == nil {
					break // successful reconnection
				}

				// Check if terminated
				if atomic.LoadInt32(&c.state) == 2 {
					c.node.Log().Info("(registrar) terminating during re-register")
					iterCancel()
					return
				}

				c.node.Log().Error("(registrar) re-register attempt %d failed: %v", attempt, err)

				// Exponential backoff with maximum
				backoff := time.Duration(attempt) * 5 * time.Second
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}

				// Interruptible wait instead of sleep
				c.node.Log().Debug("(registrar) waiting %v before retry", backoff)
				if !c.waitWithContext(iterCtx, backoff) {
					// Context cancelled (Terminate called)
					c.node.Log().Info("(registrar) backoff interrupted, terminating")
					iterCancel()
					return
				}

			}

			currentLease = c.leaseID()
		}

		// Start KeepAlive with iteration context
		keepAliveCh, err := c.cli.KeepAlive(iterCtx, currentLease)
		if err != nil {
			c.node.Log().Error("(registrar) failed to start keepalive: %v", err)
			iterCancel()

			// Interruptible wait before retry
			if !c.waitWithContext(c.ctx, 5*time.Second) {
				return
			}
			continue
		}

		// Drop lazy app-route cache before starting the new Watch.
		// During disconnect we lost events, so cached entries may be stale.
		// Clearing here (before Watch starts) avoids a window where the new
		// watcher feeds events into stale entries that we would only clear
		// afterwards. Next ResolveApplication calls will re-fetch on miss.
		c.appCacheLock.Lock()
		c.appCache = make(map[gen.Atom]*appEntry)
		c.appCacheLock.Unlock()

		// Start Watch with iteration context
		watchCh := c.cli.Watch(iterCtx, pathPrefix, etcdcli.WithPrefix())

		// Load configuration
		c.loadConfiguration()
		c.node.Log().Info("(registrar) keepalive active for lease %d", currentLease)

		// Main event loop
		disconnected := false
		for !disconnected {
			select {
			case resp, ok := <-keepAliveCh:
				if !ok {
					c.node.Log().Warning("(registrar) keepalive channel closed")
					disconnected = true
					break
				}
				if resp != nil {
					c.node.Log().Trace("(registrar) keepalive response: TTL=%d", resp.TTL)
				}

			case watchResp, ok := <-watchCh:
				if !ok {
					c.node.Log().Warning("(registrar) watch channel closed")
					disconnected = true
					break
				}

				if watchResp.Err() != nil {
					c.node.Log().Error("(registrar) watch error: %v", watchResp.Err())
					disconnected = true
					break
				}

				for _, event := range watchResp.Events {
					c.handleEvent(event)
				}
			}
		}

		// CRITICAL: Cancel context to stop keepAlive and watch goroutines
		c.node.Log().Debug("(registrar) cancelling iteration context")
		iterCancel()

		// PROPER SYNCHRONIZATION: Wait for channels to close (NO SLEEP!)
		// The etcd client will close channels when goroutines finish
		c.node.Log().Debug("(registrar) draining keepalive channel")
		for range keepAliveCh {
			// Drain remaining messages - channel will close when goroutine stops
		}

		c.node.Log().Debug("(registrar) draining watch channel")
		for range watchCh {
			// Drain remaining events - channel will close when goroutine stops
		}

		c.node.Log().Info("(registrar) all goroutines confirmed stopped")

		// Check if client was terminated
		if atomic.LoadInt32(&c.state) == 2 {
			c.node.Log().Info("(registrar) client terminated")
			return
		}

		// Reset state to unregistered for re-registration attempt
		atomic.StoreInt32(&c.state, 0)

		c.node.Log().Warning("(registrar) will attempt re-registration")
		// Continue to next iteration
	}
}

// handleEvent processes all types of events from the single watcher
func (c *client) handleEvent(event *etcdcli.Event) {
	key := string(event.Kv.Key)

	// Route based on path prefix
	switch {
	case strings.HasPrefix(key, c.pathNodes):
		c.node.Log().Debug("(registrar) node event: %s %s", event.Type, key)
		c.handleNodeEvent(event)
	case strings.HasPrefix(key, c.pathApps):
		c.node.Log().Debug("(registrar) application event: %s %s", event.Type, key)
		c.handleApplicationEvent(event)
	case strings.HasPrefix(key, c.pathConfig):
		c.node.Log().Debug("(registrar) config event: %s %s", event.Type, key)
		c.handleConfigEvent(event, c.pathConfig)
	case strings.HasPrefix(key, c.pathGlobalConfig):
		c.node.Log().Debug("(registrar) global config event: %s %s", event.Type, key)
		c.handleConfigEvent(event, c.pathGlobalConfig)
	default:
		c.node.Log().Debug("(registrar) ignoring event for unhandled path: %s", key)
	}
}

// loadConfiguration loads all configuration items from etcd
func (c *client) loadConfiguration() {
	c.configLock.Lock()
	defer c.configLock.Unlock()

	// Clear existing config
	c.config = make(map[string]any)

	// Load cluster-specific configuration
	c.loadConfigFromPath(c.pathConfig, "cluster-specific")

	// Load global configuration (cross-cluster)
	c.loadConfigFromPath(c.pathGlobalConfig, "global")

	c.node.Log().Info("(registrar) loaded %d total configuration items", len(c.config))
}

// loadConfigFromPath loads configuration items from a specific etcd path
func (c *client) loadConfigFromPath(configPath, configType string) {
	ctx, cancel := c.requestContext()
	defer cancel()

	resp, err := c.cli.Get(ctx, configPath, etcdcli.WithPrefix())
	if err != nil {
		c.node.Log().Error("(registrar) failed to load %s configuration from %s: %v", configType, configPath, err)
		return
	}

	loadedCount := 0
	nodename := string(c.node.Name())

	// Load all configuration items from this path
	for _, kv := range resp.Kvs {
		// Extract config key from etcd path
		configKey := strings.TrimPrefix(string(kv.Key), configPath)

		// Validate and normalize the configuration key format
		if !c.isValidConfigKey(configKey) {
			c.node.Log().Warning("(registrar) invalid %s config key format: %s", configType, configKey)
			continue
		}

		// Filter: only load configurations relevant to this node
		if !c.isConfigRelevantToNode(configKey, nodename) {
			c.node.Log().
				Debug("(registrar) skipping %s config not relevant to node %s: %s", configType, nodename, configKey)
			continue
		}

		// Decode the configuration value
		value, err := decodeConfigValue(string(kv.Value))
		if err != nil {
			c.node.Log().Error("(registrar) failed to decode %s config value for %s: %v", configType, configKey, err)
			continue
		}

		c.config[configKey] = value
		c.node.Log().Debug("(registrar) loaded %s config: %s = %v", configType, configKey, value)
		loadedCount++
	}

	c.node.Log().Info("(registrar) loaded %d %s configuration items", loadedCount, configType)
}

// isConfigRelevantToNode determines if a configuration key is relevant to the specified node
func (c *client) isConfigRelevantToNode(configKey, nodename string) bool {
	parts := strings.Split(configKey, "/")

	switch len(parts) {
	case 2:
		// Format: "node/item", "*/item", or "global/item"
		nodeOrScope := parts[0]
		return nodeOrScope == nodename || nodeOrScope == "*" || nodeOrScope == "global"

	case 3:
		// Format: "cluster/node/item" (cross-cluster specific)
		clusterName := parts[0]
		nodeInConfig := parts[1]

		// Only relevant if it's for this cluster and this specific node
		return clusterName == c.options.Cluster && nodeInConfig == nodename

	default:
		return false
	}
}

// handleConfigEvent processes configuration change events
func (c *client) handleConfigEvent(event *etcdcli.Event, configPath string) {
	// Extract config key from etcd path
	configKey := strings.TrimPrefix(string(event.Kv.Key), configPath)

	// Validate the configuration key format
	if !c.isValidConfigKey(configKey) {
		c.node.Log().Warning("(registrar) invalid config key format: %s", configKey)
		return
	}

	nodename := string(c.node.Name())

	// Filter: only process configuration changes relevant to this node
	if !c.isConfigRelevantToNode(configKey, nodename) {
		c.node.Log().Debug("(registrar) ignoring config change not relevant to node %s: %s", nodename, configKey)
		return
	}

	c.configLock.Lock()
	var oldValue any
	var hasOldValue bool

	if event.Type == etcdcli.EventTypeDelete {
		oldValue, hasOldValue = c.config[configKey]
		delete(c.config, configKey)
	} else {
		// EventTypePut
		oldValue, hasOldValue = c.config[configKey]

		// Decode new value
		newValue, err := decodeConfigValue(string(event.Kv.Value))
		if err != nil {
			c.node.Log().Error("(registrar) failed to decode config value for %s: %v", configKey, err)
			c.configLock.Unlock()
			return
		}

		c.config[configKey] = newValue
	}
	c.configLock.Unlock()

	// Check if this config change affects this node and send appropriate events
	c.sendConfigUpdateEvent(configKey, oldValue, hasOldValue)
}

// isValidConfigKey validates that a configuration key matches the expected hierarchical format
func (c *client) isValidConfigKey(configKey string) bool {
	if configKey == "" {
		return false
	}

	parts := strings.Split(configKey, "/")

	// Valid formats:
	// 1. "cluster/node/item" (3 parts) - cross-cluster specific
	// 2. "node/item" (2 parts) - current cluster, specific node
	// 3. "*/item" (2 parts with wildcard) - current cluster default
	// 4. "global/item" (2 parts) - global default

	switch len(parts) {
	case 2:
		// Format: "node/item", "*/item", or "global/item"
		if parts[0] == "" || parts[1] == "" {
			return false
		}
		return true

	case 3:
		// Format: "cluster/node/item" - cross-cluster specific
		if parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return false
		}
		return true

	default:
		return false
	}
}

// sendConfigUpdateEvent determines if a config change affects this node and sends events
func (c *client) sendConfigUpdateEvent(configKey string, oldValue any, hasOldValue bool) {
	nodename := string(c.node.Name())

	// Parse config key to determine if it affects this node
	// Expected formats: cluster/node/item, node/item, */item, global/item
	var itemName string
	var affects bool

	parts := strings.Split(configKey, "/")
	if len(parts) < 2 {
		return // Invalid config key format
	}

	switch len(parts) {
	case 2:
		// Format: node/item, */item, or global/item
		if parts[0] == nodename || parts[0] == "*" || parts[0] == "global" {
			itemName = parts[1]
			affects = true
		}
	case 3:
		// Format: cluster/node/item (cross-cluster specific)
		if parts[0] == c.options.Cluster && parts[1] == nodename {
			itemName = parts[2]
			affects = true
		}
	}

	if !affects {
		return
	}

	// Get the current effective value for this item (considering hierarchy)
	currentValue, err := c.ConfigItem(itemName)
	valueExists := err == nil

	// Check if the effective value has actually changed
	var valueChanged bool
	if hasOldValue && valueExists {
		// Both old and new values exist, check if they're different
		valueChanged = !compareValues(oldValue, currentValue)
	} else if hasOldValue && !valueExists {
		// Had value before, now doesn't
		valueChanged = true
	} else if !hasOldValue && valueExists {
		// Didn't have value before, now does
		valueChanged = true
	}
	// If neither had value before nor has value now, no change

	if valueChanged {
		// Send configuration update event
		ev := EventConfigUpdate{
			Item:  itemName,
			Value: currentValue,
		}

		if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
			c.node.Log().Error("(registrar) failed to send config update event: %v", err)
		} else {
			c.node.Log().Info("(registrar) sent config update event for item %s", itemName)
		}
	}
}

// compareValues compares two values for equality
func compareValues(a, b any) bool {
	// Simple equality check - could be enhanced for deep comparison if needed
	return a == b
}

// handleClusterEvent processes cluster-related events (nodes, applications)
func (c *client) handleClusterEvent(event *etcdcli.Event) {
	switch {
	case strings.HasPrefix(string(event.Kv.Key), c.pathNodes):
		c.handleNodeEvent(event)
	case strings.HasPrefix(string(event.Kv.Key), c.pathApps):
		c.handleApplicationEvent(event)
	default:
		c.node.Log().Warning("(registrar) unknown cluster event key: %s", event.Kv.Key)
	}
}

// handleNodeEvent processes node join/leave events
func (c *client) handleNodeEvent(event *etcdcli.Event) {
	// Extract node name from key
	nodeName := gen.Atom(strings.TrimPrefix(string(event.Kv.Key), c.pathNodes))

	if nodeName == c.node.Name() {
		c.node.Log().Debug("(registrar) ignoring event for self node: %s", nodeName)
		return
	}

	switch event.Type {
	case etcdcli.EventTypePut:
		// Node joined
		ev := EventNodeJoined{Name: nodeName}
		if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
			c.node.Log().Error("(registrar) failed to send node joined event: %v", err)
		} else {
			c.node.Log().Info("(registrar) node %s joined cluster", nodeName)
		}

	case etcdcli.EventTypeDelete:
		// Node left
		ev := EventNodeLeft{Name: nodeName}
		if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
			c.node.Log().Error("(registrar) failed to send node left event: %v", err)
		} else {
			c.node.Log().Info("(registrar) node %s left cluster", nodeName)
		}
	}
}

// handleApplicationEvent processes application lifecycle events
func (c *client) handleApplicationEvent(event *etcdcli.Event) {
	// Extract application name and node from key
	// Format: pathApps + appName + "/" + nodeName
	keyWithoutPrefix := strings.TrimPrefix(string(event.Kv.Key), c.pathApps)
	parts := strings.Split(keyWithoutPrefix, "/")

	if len(parts) != 2 {
		c.node.Log().Warning("(registrar) invalid application key format: %s", event.Kv.Key)
		return
	}

	appName := gen.Atom(parts[0])
	nodeName := gen.Atom(parts[1])

	switch event.Type {
	case etcdcli.EventTypePut:
		// Application started/updated
		route, err := decode(event.Kv.Value)
		if err != nil {
			c.node.Log().Error("(registrar) failed to decode application route: %v", err)
			return
		}

		appRoute, ok := route.(gen.ApplicationRoute)
		if ok == false {
			c.node.Log().Error("(registrar) invalid application route type: %T", route)
			return
		}

		// Update cache before emitting the framework event so receivers
		// that call ResolveApplication see the fresh state.
		c.updateAppCachePut(appName, nodeName, appRoute, event.Kv.ModRevision)

		// Send appropriate event based on application state
		switch appRoute.State {
		case gen.ApplicationStateLoaded:
			ev := EventApplicationLoaded{
				Name:   appName,
				Node:   nodeName,
				Weight: appRoute.Weight,
			}
			if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
				c.node.Log().Error("(registrar) failed to send application loaded event: %v", err)
			}

		case gen.ApplicationStateRunning:
			ev := EventApplicationStarted{
				Name:   appName,
				Node:   nodeName,
				Weight: appRoute.Weight,
				Mode:   appRoute.Mode,
			}
			if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
				c.node.Log().Error("(registrar) failed to send application started event: %v", err)
			}

		case gen.ApplicationStateStopping:
			ev := EventApplicationStopping{
				Name: appName,
				Node: nodeName,
			}
			if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
				c.node.Log().Error("(registrar) failed to send application stopping event: %v", err)
			}
		}

	case etcdcli.EventTypeDelete:
		// Update cache before emitting the framework event.
		c.updateAppCacheDelete(appName, nodeName, event.Kv.ModRevision)

		// Application stopped/unloaded
		ev := EventApplicationStopped{
			Name: appName,
			Node: nodeName,
		}
		if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, ev); err != nil {
			c.node.Log().Error("(registrar) failed to send application stopped event: %v", err)
		}
	}
}

// updateAppCachePut applies a PUT watch event to the app cache if it's newer
// than the entry's stored revision. Entries with no prior resolve are ignored
// (a future ResolveApplication will fetch fresh state on miss).
func (c *client) updateAppCachePut(appName, nodeName gen.Atom, route gen.ApplicationRoute, rev int64) {
	c.appCacheLock.Lock()
	defer c.appCacheLock.Unlock()

	entry, ok := c.appCache[appName]
	if ok == false {
		return
	}
	if rev <= entry.rev {
		return
	}
	if entry.routes == nil {
		entry.routes = make(map[gen.Atom]gen.ApplicationRoute)
	}
	entry.routes[nodeName] = route
	entry.rev = rev
	entry.rrGen++
}

// updateAppCacheDelete applies a DELETE watch event to the app cache.
func (c *client) updateAppCacheDelete(appName, nodeName gen.Atom, rev int64) {
	c.appCacheLock.Lock()
	defer c.appCacheLock.Unlock()

	entry, ok := c.appCache[appName]
	if ok == false {
		return
	}
	if rev <= entry.rev {
		return
	}
	delete(entry.routes, nodeName)
	entry.rev = rev
	entry.rrGen++
}

func (c *client) tryRegister() (gen.StaticRoutes, error) {
	var noStaticRoutes gen.StaticRoutes

	if atomic.LoadInt32(&c.state) == 2 {
		return noStaticRoutes, gen.ErrRegistrarTerminated
	}

	grantCtx, grantCancel := c.requestContext()
	leaseResponse, err := c.cli.Grant(grantCtx, c.leaseTTL)
	grantCancel()
	if err != nil {
		return noStaticRoutes, err
	}
	lease := leaseResponse.ID
	c.setLeaseID(lease)

	key := c.pathNodes + string(c.node.Name())
	value, err := encode(c.routes)
	if err != nil {
		// Clean up lease on encode error
		c.revokeLease(lease)
		c.setLeaseID(0)
		return noStaticRoutes, err
	}

	// register node with routes (protected: only if key doesn't exist)
	txCtx, txCancel := c.requestContext()
	txResult, err := c.cli.Txn(txCtx).
		If(etcdcli.Compare(etcdcli.CreateRevision(key), "=", 0)).
		Then(etcdcli.OpPut(key, value, etcdcli.WithLease(lease))).
		Commit()
	txCancel()

	if err != nil {
		// Clean up lease on transaction error
		c.revokeLease(lease)
		c.setLeaseID(0)
		return noStaticRoutes, err
	}

	if txResult.Succeeded == false {
		// Clean up lease if key already exists
		c.revokeLease(lease)
		c.setLeaseID(0)
		return noStaticRoutes, gen.ErrTaken
	}

	atomic.StoreInt32(&c.state, 1) // set state to registered

	c.apps.Range(func(key any, value any) bool {
		if err := c.RegisterApplicationRoute(value.(gen.ApplicationRoute)); err != nil {
			c.node.Log().Error("(registrar) unable to register application route: %s", err)
		}
		return true
	})

	return noStaticRoutes, nil
}

// tryReRegister performs re-registration after disconnect.
// Creates a new lease FIRST, then attempts to register using two strategies:
// 1. Key doesn't exist (old lease expired) - create it
// 2. Key exists with our old lease (reconnected before expiry) - replace lease
// Old lease is revoked only AFTER the node key and every application route
// have been re-attached to the new lease.
func (c *client) tryReRegister(oldLease etcdcli.LeaseID) error {
	if atomic.LoadInt32(&c.state) == 2 {
		return gen.ErrRegistrarTerminated
	}

	key := c.pathNodes + string(c.node.Name())

	// Create new lease first (before touching old one)
	grantCtx, grantCancel := c.requestContext()
	leaseResponse, err := c.cli.Grant(grantCtx, c.leaseTTL)
	grantCancel()
	if err != nil {
		return fmt.Errorf("failed to create new lease: %w", err)
	}
	newLease := leaseResponse.ID
	c.node.Log().Debug("(registrar) created new lease %d", newLease)

	value, err := encode(c.routes)
	if err != nil {
		c.revokeLease(newLease)
		return fmt.Errorf("failed to encode routes: %w", err)
	}

	// Attempt 1: key doesn't exist (old lease expired, key was deleted)
	tx1Ctx, tx1Cancel := c.requestContext()
	txResult, err := c.cli.Txn(tx1Ctx).
		If(etcdcli.Compare(etcdcli.CreateRevision(key), "=", 0)).
		Then(etcdcli.OpPut(key, value, etcdcli.WithLease(newLease))).
		Commit()
	tx1Cancel()
	if err != nil {
		c.revokeLease(newLease)
		return fmt.Errorf("failed to execute transaction: %w", err)
	}

	// Attempt 2: key exists with our old lease (reconnected before expiry)
	if txResult.Succeeded == false && oldLease != 0 {
		tx2Ctx, tx2Cancel := c.requestContext()
		txResult, err = c.cli.Txn(tx2Ctx).
			If(etcdcli.Compare(etcdcli.LeaseValue(key), "=", int64(oldLease))).
			Then(etcdcli.OpPut(key, value, etcdcli.WithLease(newLease))).
			Commit()
		tx2Cancel()
		if err != nil {
			c.revokeLease(newLease)
			return fmt.Errorf("failed to execute transaction: %w", err)
		}
	}

	if txResult.Succeeded == false {
		// Both attempts failed - another node registered this name
		c.revokeLease(newLease)
		c.node.Log().Error("(registrar) key was taken by another node during re-registration")
		return gen.ErrTaken
	}

	// Success
	c.setLeaseID(newLease)
	atomic.StoreInt32(&c.state, 1)
	c.node.Log().Info("(registrar) successfully re-registered with lease %d", newLease)

	// Re-attach application routes to the new lease BEFORE revoking the old one.
	// A Put with the new lease rewrites the existing key, so watchers observe a
	// PUT and their resolve caches keep the route. Revoking first would delete
	// every app key still held by the old lease, making a healthy node look like
	// it left the cluster until these Puts land.
	reattached := true
	c.apps.Range(func(k any, v any) bool {
		if err := c.RegisterApplicationRoute(v.(gen.ApplicationRoute)); err != nil {
			c.node.Log().Error("(registrar) unable to register application route: %s", err)
			reattached = false
		}
		return true
	})

	if reattached == true {
		c.revokeLease(oldLease)
		return nil
	}

	// Some app keys are still on the old lease. Let it expire by TTL instead of
	// turning a failed re-attach into a delete event for those routes.
	c.node.Log().Warning("(registrar) not all application routes were re-attached, leaving lease %d to expire", oldLease)

	return nil
}

// waitWithContext waits for duration or until context is cancelled
func (c *client) waitWithContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true // normal completion
	case <-ctx.Done():
		return false // interrupted
	}
}
