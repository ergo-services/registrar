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
	c.cli.Put(context.Background(), key, value, etcdcli.WithLease(c.lease))
	return nil
}
func (c *client) UnregisterApplicationRoute(name gen.Atom) error {
	c.apps.Delete(name)
	key := c.pathApps + string(name) + "/" + string(c.node.Name())
	c.cli.Delete(context.Background(), key)
	return nil
}

func (c *client) Nodes() ([]gen.Atom, error) {
	resp, err := c.cli.Get(context.Background(), c.pathNodes, etcdcli.WithPrefix())
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
	if c.lease != 0 {
		c.cli.Revoke(context.Background(), c.lease) // revoke the lease
	}
	c.cli.Close() // close the client connection
	if c.node != nil {
		c.node.Log().Trace("(etcd) registrar client terminated")
	}
}

// internals

func (c *client) keepRegistration() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keepAliveCh, err := c.cli.KeepAlive(ctx, c.lease)
	if err != nil {
		panic(err)
	}

	// Single watcher for all events - watch the common path prefix
	watchCh := c.cli.Watch(ctx, pathPrefix, etcdcli.WithPrefix())

	// Load initial configuration from both sources
	c.loadConfiguration()

	for {
		select {
		case _, ok := <-keepAliveCh:
			if ok == false {
				break
			}
			continue

		case watchResp, ok := <-watchCh:
			if ok == false {
				break
			}

			for _, event := range watchResp.Events {
				c.handleEvent(event)
			}
			continue
		}

		// disconnected, try to reconnect
		if old := atomic.SwapInt32(&c.state, 0); old == 2 {
			return
		}

		for {
			if _, err := c.tryRegister(); err != nil {
				c.node.Log().Error("(registrar) failed to re-register: %v", err)
				time.Sleep(5 * time.Second) // wait before retrying
				continue
			}

			keepAliveCh, err = c.cli.KeepAlive(ctx, c.lease)
			if err != nil {
				panic(err)
			}

			watchCh = c.cli.Watch(ctx, pathPrefix, etcdcli.WithPrefix())

			// Reload configuration after reconnection
			c.loadConfiguration()
			break
		}
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
	resp, err := c.cli.Get(context.Background(), configPath, etcdcli.WithPrefix())
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
		if !ok {
			c.node.Log().Error("(registrar) invalid application route type: %T", route)
			return
		}

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

func (c *client) tryRegister() (gen.StaticRoutes, error) {
	var noStaticRoutes gen.StaticRoutes

	if atomic.LoadInt32(&c.state) == 2 {
		return noStaticRoutes, gen.ErrRegistrarTerminated
	}

	leaseResponse, err := c.cli.Grant(context.Background(), 10) // 10 seconds lease
	if err != nil {
		return noStaticRoutes, err
	}
	c.lease = leaseResponse.ID

	key := c.pathNodes + string(c.node.Name())
	value, err := encode(c.routes)
	if err != nil {
		return noStaticRoutes, err
	}

	// register node with routes
	tx := c.cli.Txn(context.Background())
	txResult, err := tx.
		If(etcdcli.Compare(etcdcli.CreateRevision(key), "=", 0)).
		Then(etcdcli.OpPut(key, value, etcdcli.WithLease(c.lease))).
		Commit()

	if err != nil {
		return noStaticRoutes, err
	}

	if txResult.Succeeded == false {
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
