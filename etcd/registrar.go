package etcd

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
	etcdcli "go.etcd.io/etcd/client/v3"
)

var (
	errLeaseLost = fmt.Errorf("etcd lease lost")
	errWatchLost = fmt.Errorf("etcd watch could not be restored")
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

// Nodes answers from the mirror: no RPC, cannot block. Suspects are listed so
// membership, resolve and events tell one story; a clean shutdown announces
// itself and is gone from here at once.
func (c *client) Nodes() ([]gen.Atom, error) {
	c.mirror.lock.RLock()
	if c.mirror.seeded {
		nodes := make([]gen.Atom, 0, len(c.mirror.nodes))
		for name := range c.mirror.nodes {
			if name == c.node.Name() {
				continue // skip self
			}
			nodes = append(nodes, name)
		}
		c.mirror.lock.RUnlock()
		sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
		return nodes, nil
	}
	c.mirror.lock.RUnlock()

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

	// Announce and remove before the context is cancelled: after c.cancel()
	// every request derived from it fails immediately.
	c.departGracefully()

	c.cancel() // cancel main context - stops KeepAlive immediately
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

// requestContext bounds one etcd RPC. clientv3 defaults to WaitForReady(true),
// so a call without a deadline blocks for as long as etcd is unreachable.
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
			c.statSessionRebuilt.Add(1)
			c.node.Log().Info("(registrar) attempting to re-register (iteration %d). "+
				"sessions revived %d, rebuilt %d; watches resumed %d, resynced %d; "+
				"mirror seeded %d times; routes resolved from suspect %d, expired after grace %d",
				iteration,
				c.statSessionRevived.Load(), c.statSessionRebuilt.Load(),
				c.statWatchResumed.Load(), c.statWatchResync.Load(), c.statSeeds.Load(),
				c.statResolvedFromSuspect.Load(), c.statExpiredAfterGrace.Load())

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

		// Snapshot first, then watch from seedRev+1: the handover misses nothing
		// and replays nothing. reconcile decides per entry what a gone key means.
		seedRev, err := c.seedMirror()
		if err != nil {
			c.node.Log().Error("(registrar) failed to seed the cluster mirror: %v", err)
			iterCancel()
			if !c.waitWithContext(c.ctx, 5*time.Second) {
				return
			}
			continue
		}

		watchRev := seedRev + 1
		watchCh := c.startWatch(iterCtx, watchRev)

		// Load configuration
		c.loadConfiguration()
		c.node.Log().Info("(registrar) keepalive active for lease %d", currentLease)

		// The clock only runs while the watch is up: with no session we have no
		// evidence about anyone.
		sweepTicker := time.NewTicker(c.options.SweepInterval)

		c.sendEvent(EventRegistrarConnected{Info: c.Info()})

		// Main event loop
		disconnected := false
		disconnectReason := error(nil)
		watchFailures := 0
		for !disconnected {
			select {
			case resp, ok := <-keepAliveCh:
				if !ok {
					// Not necessarily a lost lease. Ask before rebuilding.
					revived, ch := c.reviveLease(iterCtx, currentLease)
					if revived == false {
						disconnected = true
						disconnectReason = errLeaseLost
						break
					}
					keepAliveCh = ch
					continue
				}
				if resp != nil {
					c.node.Log().Trace("(registrar) keepalive response: TTL=%d", resp.TTL)
				}

			case <-sweepTicker.C:
				c.sweepMirror()

			case watchResp, ok := <-watchCh:
				if ok == false || watchResp.Err() != nil {
					// A broken watch is not a broken registration.
					ch, rev, recovered := c.recoverWatch(iterCtx, watchResp, ok, watchRev, &watchFailures)
					if recovered == false {
						disconnected = true
						disconnectReason = errWatchLost
						break
					}
					watchCh, watchRev = ch, rev
					continue
				}

				watchFailures = 0
				c.applyWatchResponse(watchResp)
				watchRev = watchNextRev(watchResp, watchRev)
			}
		}

		sweepTicker.Stop()

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
		c.sendEvent(EventRegistrarDisconnected{Reason: disconnectReason})

		c.node.Log().Warning("(registrar) will attempt re-registration")
		// Continue to next iteration
	}
}

// departGracefully announces the shutdown before removing this node's keys:
// without it a rolling deploy looks exactly like the failure the grace exists
// for. A separate key, not a flag in the node value, so older registrars ignore
// it instead of failing to decode. Best effort, the lease revoke covers it.
func (c *client) departGracefully() {
	if c.node == nil {
		return
	}

	name := string(c.node.Name())

	ctx, cancel := context.WithTimeout(context.Background(), c.options.RequestTimeout)
	defer cancel()

	marker, err := c.cli.Grant(ctx, c.leaseTTL)
	if err != nil {
		c.node.Log().Debug("(registrar) unable to announce departure: %s", err)
		return
	}
	if _, err := c.cli.Put(ctx, c.pathLeaving+name, "", etcdcli.WithLease(marker.ID)); err != nil {
		c.node.Log().Debug("(registrar) unable to announce departure: %s", err)
		return
	}

	ops := make([]etcdcli.Op, 0, 4)
	c.apps.Range(func(app any, _ any) bool {
		ops = append(ops, etcdcli.OpDelete(c.pathApps+string(app.(gen.Atom))+"/"+name))
		return true
	})
	ops = append(ops, etcdcli.OpDelete(c.pathNodes+name))

	if _, err := c.cli.Txn(ctx).Then(ops...).Commit(); err != nil {
		c.node.Log().Debug("(registrar) unable to remove own keys on shutdown: %s", err)
		return
	}

	c.node.Log().Info("(registrar) announced departure and removed %d key(s)", len(ops))
}

// startWatch opens the cluster watch. WithProgressNotify keeps the resume point
// moving while the cluster is quiet, so a reconnect does not ask for a revision
// old enough to have been compacted away.
func (c *client) startWatch(ctx context.Context, rev int64) etcdcli.WatchChan {
	return c.cli.Watch(ctx, pathPrefix,
		etcdcli.WithPrefix(),
		etcdcli.WithRev(rev),
		etcdcli.WithProgressNotify(),
	)
}

// watchNextRev follows the rule clientv3 uses internally (watch.go:851-856):
// past the last event when there are events, past the header otherwise. Taking
// the header while events are present would skip everything in between.
func watchNextRev(resp etcdcli.WatchResponse, current int64) int64 {
	if n := len(resp.Events); n > 0 {
		return resp.Events[n-1].Kv.ModRevision + 1
	}
	if resp.Header == nil { // only a response we built ourselves
		return current
	}
	if resp.Header.Revision > 0 {
		return resp.Header.Revision + 1
	}
	return current
}

// reviveLease re-arms KeepAlive on the same lease. clientv3 closes that channel
// both on a real expiry (lease.go:530) and after one TTL of silence
// (deadlineLoop, lease.go:558); re-arming asks the server instead of guessing.
func (c *client) reviveLease(ctx context.Context, lease etcdcli.LeaseID) (bool, <-chan *etcdcli.LeaseKeepAliveResponse) {
	c.node.Log().Warning("(registrar) keepalive channel closed, checking whether lease %d is still alive", lease)

	ch, err := c.cli.KeepAlive(ctx, lease)
	if err != nil {
		c.node.Log().Warning("(registrar) lease %d cannot be kept alive: %v", lease, err)
		return false, nil
	}

	// One TTL: no answer within it means the lease is expiring anyway.
	budget := time.Duration(c.leaseTTL) * time.Second
	timer := time.NewTimer(budget)
	defer timer.Stop()

	select {
	case resp, ok := <-ch:
		if ok == false || resp == nil || resp.TTL <= 0 {
			c.node.Log().Warning("(registrar) lease %d is gone, re-registering", lease)
			return false, nil
		}
		c.statSessionRevived.Add(1)
		c.node.Log().Info("(registrar) session recovered on lease %d, TTL=%d, nothing republished", lease, resp.TTL)
		return true, ch

	case <-timer.C:
		c.node.Log().Warning("(registrar) lease %d did not answer within %v, re-registering", lease, budget)
		return false, nil

	case <-ctx.Done():
		return false, nil
	}
}

// recoverWatch rebuilds the watch stream in place, leaving the session alone.
// Returns false when recovery keeps failing and the session must be rebuilt.
// ok reports whether the channel is still open.
func (c *client) recoverWatch(
	ctx context.Context,
	resp etcdcli.WatchResponse,
	ok bool,
	rev int64,
	failures *int,
) (etcdcli.WatchChan, int64, bool) {
	*failures++

	switch {
	case ok == false:
		c.node.Log().Warning("(registrar) watch channel closed, resuming at revision %d", rev)
	case resp.CompactRevision > 0:
		// Position compacted away, only a fresh snapshot can catch up.
		c.node.Log().Warning("(registrar) watch revision %d compacted away (compact revision %d), resyncing",
			rev, resp.CompactRevision)
		seedRev, err := c.seedMirror()
		if err != nil {
			c.node.Log().Error("(registrar) resync after compaction failed: %v", err)
			break
		}
		c.statWatchResync.Add(1)
		rev = seedRev + 1
		*failures = 0
	default:
		c.node.Log().Warning("(registrar) watch failed: %v, resuming at revision %d", resp.Err(), rev)
	}

	if *failures > watchRecoveryAttempts {
		c.node.Log().Error("(registrar) watch could not be restored in %d attempts, rebuilding the session",
			watchRecoveryAttempts)
		return nil, rev, false
	}

	// Full jitter: a fleet must not come back in lockstep.
	shift := *failures - 1
	if shift < 0 {
		shift = 0 // a successful resync cleared the counter
	}
	if shift > 8 {
		shift = 8
	}
	backoff := watchRetryMin << shift
	if backoff > watchRetryMax {
		backoff = watchRetryMax
	}
	if backoff > 0 {
		backoff = time.Duration(rand.Int63n(int64(backoff)) + 1)
	}
	if c.waitWithContext(ctx, backoff) == false {
		return nil, rev, false
	}

	c.statWatchResumed.Add(1)
	return c.startWatch(ctx, rev), rev, true
}

// seedMirror loads the cluster state into the mirror and returns the revision
// the snapshot was taken at. The watch must then be started at that revision
// plus one.
func (c *client) seedMirror() (int64, error) {
	ctx, cancel := c.requestContext()
	defer cancel()

	// One transaction so every range is read at the same revision, and scoped to
	// this cluster: the watch prefix is shared, a bulk read must not be.
	resp, err := c.cli.Txn(ctx).Then(
		etcdcli.OpGet(c.pathNodes, etcdcli.WithPrefix()),
		etcdcli.OpGet(c.pathApps, etcdcli.WithPrefix()),
		etcdcli.OpGet(c.pathLeaving, etcdcli.WithPrefix()),
	).Commit()
	if err != nil {
		return 0, err
	}

	nodes := make(map[gen.Atom][]gen.Route)
	for _, kv := range resp.Responses[0].GetResponseRange().Kvs {
		name := gen.Atom(strings.TrimPrefix(string(kv.Key), c.pathNodes))
		routes, err := decodeRoutes(kv.Value)
		if err != nil {
			c.node.Log().Error("(registrar) failed to decode routes of node %s: %v", name, err)
			continue
		}
		nodes[name] = routes
	}

	apps := make(map[gen.Atom]map[gen.Atom]gen.ApplicationRoute)
	for _, kv := range resp.Responses[1].GetResponseRange().Kvs {
		appName, nodeName, ok := c.splitApplicationKey(string(kv.Key))
		if ok == false {
			continue
		}
		route, err := decodeApplicationRoute(kv.Value)
		if err != nil {
			c.node.Log().Error("(registrar) failed to decode application route %s: %v", kv.Key, err)
			continue
		}
		if apps[appName] == nil {
			apps[appName] = make(map[gen.Atom]gen.ApplicationRoute)
		}
		apps[appName][nodeName] = route
	}

	leaving := make(map[gen.Atom]struct{})
	for _, kv := range resp.Responses[2].GetResponseRange().Kvs {
		leaving[gen.Atom(strings.TrimPrefix(string(kv.Key), c.pathLeaving))] = struct{}{}
	}

	c.mirror.reconcile(nodes, apps, leaving, resp.Header.Revision, c.graceTicks)
	c.statSeeds.Add(1)
	c.node.Log().Info("(registrar) mirror seeded at revision %d: %d node(s), %d application(s), %d suspect route(s)",
		resp.Header.Revision, len(nodes), len(apps), c.mirror.suspectCount())

	return resp.Header.Revision, nil
}

// sweepMirror advances the suspicion clock and publishes what the grace
// confirmed. An inferred loss is announced only here, so a node that comes back
// in time costs the cluster no event at all.
func (c *client) sweepMirror() {
	routes, nodes := c.mirror.sweep()
	if len(routes) == 0 && len(nodes) == 0 {
		return
	}

	c.statExpiredAfterGrace.Add(int64(len(routes)))
	c.node.Log().Info("(registrar) suspicion expired, dropped %d application route(s) and %d node(s)",
		len(routes), len(nodes))

	for _, route := range routes {
		c.sendEvent(EventApplicationStopped{Name: route.app, Node: route.node})
	}
	for _, name := range nodes {
		if name == c.node.Name() {
			continue
		}
		c.node.Log().Info("(registrar) node %s left cluster", name)
		c.sendEvent(EventNodeLeft{Name: name})
	}
}

// applyWatchResponse applies one watch response and publishes what it implies.
// Events are grouped by revision, node keys first: a lease expiry removes both
// in one revision with no ordering inside it, so classifying application
// deletes against the node state of that revision is the only stable rule.
func (c *client) applyWatchResponse(resp etcdcli.WatchResponse) {
	// Decode before the lock, publish after it: resolve readers must not wait.
	parsed := c.parseWatchEvents(resp.Events)

	var pending []any

	c.mirror.lock.Lock()
	for start := 0; start < len(parsed); {
		rev := parsed[start].rev
		end := start
		for end < len(parsed) && parsed[end].rev == rev {
			end++
		}
		group := parsed[start:end]
		start = end

		// Announcements first: they decide how deletions in the same revision read.
		for _, event := range group {
			if event.kind == eventLeaving {
				c.mirror.setLeaving(event.node, event.put)
			}
		}
		for _, event := range group {
			if event.kind == eventNode {
				pending = append(pending, c.applyNodeEvent(event)...)
			}
		}
		for _, event := range group {
			if event.kind == eventApplication {
				pending = append(pending, c.applyApplicationEvent(event)...)
			}
		}

		if rev > c.mirror.rev {
			c.mirror.rev = rev
		}
	}
	c.mirror.lock.Unlock()

	for _, event := range parsed {
		switch event.kind {
		case eventClusterConfig:
			c.handleConfigEvent(event.raw, c.pathConfig)
		case eventGlobalConfig:
			c.handleConfigEvent(event.raw, c.pathGlobalConfig)
		}
	}

	for _, event := range pending {
		switch ev := event.(type) {
		case EventNodeJoined:
			c.node.Log().Info("(registrar) node %s joined cluster", ev.Name)
		case EventNodeLeft:
			c.node.Log().Info("(registrar) node %s left cluster", ev.Name)
		}
		c.sendEvent(event)
	}
}

const (
	eventOther = iota
	eventLeaving
	eventNode
	eventApplication
	eventClusterConfig
	eventGlobalConfig
)

// watchEvent is a decoded watch event, ready to apply.
type watchEvent struct {
	kind   int
	rev    int64
	put    bool
	node   gen.Atom
	app    gen.Atom
	routes []gen.Route          // node key value
	route  gen.ApplicationRoute // application key value
	raw    *etcdcli.Event       // config events are handled as they were
}

// parseWatchEvents decodes and classifies a batch, dropping malformed entries.
func (c *client) parseWatchEvents(events []*etcdcli.Event) []watchEvent {
	parsed := make([]watchEvent, 0, len(events))

	for _, event := range events {
		key := string(event.Kv.Key)
		put := event.Type == etcdcli.EventTypePut
		item := watchEvent{rev: event.Kv.ModRevision, put: put, raw: event}

		switch {
		case strings.HasPrefix(key, c.pathLeaving):
			item.kind = eventLeaving
			item.node = gen.Atom(strings.TrimPrefix(key, c.pathLeaving))

		case strings.HasPrefix(key, c.pathNodes):
			item.kind = eventNode
			item.node = gen.Atom(strings.TrimPrefix(key, c.pathNodes))
			if put {
				routes, err := decodeRoutes(event.Kv.Value)
				if err != nil {
					c.node.Log().Error("(registrar) failed to decode routes of node %s: %v", item.node, err)
					continue
				}
				item.routes = routes
			}

		case strings.HasPrefix(key, c.pathApps):
			app, node, ok := c.splitApplicationKey(key)
			if ok == false {
				continue
			}
			item.kind = eventApplication
			item.app, item.node = app, node
			if put {
				route, err := decodeApplicationRoute(event.Kv.Value)
				if err != nil {
					c.node.Log().Error("(registrar) failed to decode application route %s: %v", key, err)
					continue
				}
				item.route = route
			}

		case strings.HasPrefix(key, c.pathConfig):
			c.node.Log().Debug("(registrar) config event: %s %s", event.Type, key)
			item.kind = eventClusterConfig

		case strings.HasPrefix(key, c.pathGlobalConfig):
			c.node.Log().Debug("(registrar) global config event: %s %s", event.Type, key)
			item.kind = eventGlobalConfig

		default:
			c.node.Log().Debug("(registrar) ignoring event for unhandled path: %s", key)
			continue
		}

		parsed = append(parsed, item)
	}

	return parsed
}

// sendEvent publishes one registrar event.
func (c *client) sendEvent(event any) {
	if err := c.node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, event); err != nil {
		c.node.Log().Error("(registrar) failed to send %T: %v", event, err)
	}
}

// splitApplicationKey extracts the application and node names.
func (c *client) splitApplicationKey(key string) (gen.Atom, gen.Atom, bool) {
	parts := strings.Split(strings.TrimPrefix(key, c.pathApps), "/")
	if len(parts) != 2 {
		c.node.Log().Warning("(registrar) invalid application key format: %s", key)
		return "", "", false
	}
	return gen.Atom(parts[0]), gen.Atom(parts[1]), true
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

// applyNodeEvent updates the mirror and returns the events to publish. The node
// key is the liveness signal of its owner.
// Caller must hold c.mirror.lock.
func (c *client) applyNodeEvent(event watchEvent) []any {
	self := event.node == c.node.Name()

	if event.put {
		c.mirror.putNode(event.node, event.routes)
		if self {
			return nil
		}
		return []any{EventNodeJoined{Name: event.node}}
	}

	if c.mirror.isLeaving(event.node) == false {
		// Not the owner's word, so nothing is announced yet: the sweep will, if
		// the node is still gone when the grace ends.
		c.mirror.suspectNode(event.node, c.graceTicks())
		return nil
	}

	c.mirror.removeNode(event.node)
	if self {
		return nil
	}
	return []any{EventNodeLeft{Name: event.node}}
}

// Caller must hold c.mirror.lock.
func (c *client) applyApplicationEvent(event watchEvent) []any {
	if event.put {
		c.mirror.putAppRoute(event.app, event.node, event.route, event.rev)

		switch event.route.State {
		case gen.ApplicationStateLoaded:
			return []any{EventApplicationLoaded{
				Name:   event.app,
				Node:   event.node,
				Weight: event.route.Weight,
			}}
		case gen.ApplicationStateRunning:
			return []any{EventApplicationStarted{
				Name:   event.app,
				Node:   event.node,
				Weight: event.route.Weight,
				Mode:   event.route.Mode,
			}}
		case gen.ApplicationStateStopping:
			return []any{EventApplicationStopping{Name: event.app, Node: event.node}}
		}
		return nil
	}

	if c.mirror.nodeLive(event.node) == false && c.mirror.isLeaving(event.node) == false {
		// Nobody vouched for this: keep serving it, deprioritized, and stay
		// quiet until the grace runs out.
		c.mirror.suspectAppRoute(event.app, event.node, c.graceTicks(), event.rev)
		return nil
	}

	// The owner is there, or announced its departure: its own doing.
	c.mirror.removeAppRoute(event.app, event.node, event.rev)
	return []any{EventApplicationStopped{Name: event.app, Node: event.node}}
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

// tryReRegister takes a new lease first, then claims the node key either as a
// fresh key (old lease expired) or by replacing the old lease on it. The old
// lease is revoked only after every key has moved to the new one.
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

	// Re-attach before revoking: a Put with the new lease rewrites the key, so
	// watchers see a PUT. Revoking first would delete every app key still held by
	// the old lease and make a healthy node look like it left.
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
