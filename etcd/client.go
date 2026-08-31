package etcd

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
	etcdversion "go.etcd.io/etcd/api/v3/version"
	etcdcli "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	pathPrefix              = "services/ergo"
	formatPathCluster       = pathPrefix + "/cluster/%s/"
	formatPathClusterRoutes = pathPrefix + "/cluster/%s/routes/" // Non-overlapping with config
	formatPathNodes         = pathPrefix + "/cluster/%s/routes/nodes/"
	formatPathApps          = pathPrefix + "/cluster/%s/routes/applications/"
	formatPathLeaving       = pathPrefix + "/cluster/%s/routes/leaving/"
	formatPathConfig        = pathPrefix + "/cluster/%s/config/"
	formatPathGlobalConfig  = pathPrefix + "/config/"

	// Default configuration values
	defaultDialTimeout    = 10 * time.Second
	defaultRequestTimeout = 10 * time.Second
	defaultKeepAlive      = 10 * time.Second

	// Suspicion lifetime and the resolution of its clock.
	defaultSuspectGrace  = 30 * time.Second
	defaultSweepInterval = time.Second

	// A broken watch is rebuilt in place; the session only after this many fails.
	watchRetryMin         = 100 * time.Millisecond
	watchRetryMax         = 2 * time.Second
	watchRecoveryAttempts = 5
)

// Configuration Key Format:
// Configuration items are stored in etcd using hierarchical paths with the following formats:
//
// Within each cluster's config path (services/ergo/cluster/{cluster}/config/):
//
// 1. Node-specific within cluster: "node/item"
//    Example: "web1/database.timeout"
//    etcd path: services/ergo/cluster/production/config/web1/database.timeout
//
// 2. Cluster-wide default: "*/item"
//    Example: "*/log.level"
//    etcd path: services/ergo/cluster/production/config/*/log.level
//
// For cross-cluster configurations (services/ergo/config/):
//
// 3. Cross-cluster node-specific: "cluster/node/item"
//    Example: "production/web1/database.host"
//    etcd path: services/ergo/config/production/web1/database.host
//
// 4. Global default: "global/item"
//    Example: "global/debug.enabled"
//    etcd path: services/ergo/config/global/debug.enabled
//
// Resolution priority (highest to lowest): 3 -> 1 -> 2 -> 4

var (
	defaultEndpoins = []string{"localhost:2379"}
)

// appEntry holds every known route of one application, keyed by node name.
// rev is the revision the entry was last known accurate at.
type appEntry struct {
	routes  map[gen.Atom]gen.ApplicationRoute // keyed by node name
	suspect map[gen.Atom]int                  // node -> grace ticks left, nil when healthy
	rev     int64

	// rrGen is bumped under appCacheLock whenever routes is mutated.
	// rrState/rrSeen are protected by rrLock and rebuilt lazily when
	// rrSeen != observed rrGen. Separating rrLock from appCacheLock keeps
	// the read-mostly cache path free of write-lock contention during
	// the smooth-WRR step that mutates rotation state.
	rrGen   uint64
	rrLock  sync.Mutex
	rrSeen  uint64
	rrState map[gen.Atom]int // smooth-WRR current_weight
}

type client struct {
	options Options

	cli *etcdcli.Client

	// Replaced by keepRegistration while callers read it, hence leaseID/setLeaseID.
	lease    atomic.Int64
	leaseTTL int64 // TTL for etcd lease in seconds

	node gen.NodeRegistrar

	ctx    context.Context
	cancel context.CancelFunc

	pathCluster       string
	pathClusterRoutes string // Non-overlapping with config - uses edf.Encode + base64
	pathNodes         string
	pathApps          string
	pathLeaving       string // departure markers, see departGracefully
	pathConfig        string // Uses string encoding with type prefixes
	pathGlobalConfig  string

	routes []gen.Route

	config     map[string]any
	configLock sync.RWMutex
	apps       sync.Map // map[gen.Atom]gen.ApplicationRoute — local apps, kept for re-registration

	mirror *mirror // full in-memory view of the cluster, seeded and watched

	event    gen.Event
	eventRef gen.Ref

	state int32 // 0 unregistered, 1 registered, 2 terminated

	// counters, read for diagnostics only
	statResolvedFromSuspect atomic.Int64
	statExpiredAfterGrace   atomic.Int64
	statSeeds               atomic.Int64
	statSessionRevived      atomic.Int64 // keepalive resumed on the same lease
	statSessionRebuilt      atomic.Int64 // lease was really gone, re-registered
	statWatchResumed        atomic.Int64 // watch rebuilt without touching the session
	statWatchResync         atomic.Int64 // watch position compacted away
}

// Options for ETCD registrar with authentication and security support
type Options struct {
	Cluster   string
	Endpoints []string

	// Authentication options
	Username string
	Password string

	// TLS/Security options
	TLS                *tls.Config
	InsecureSkipVerify bool

	// Connection options
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	KeepAlive      time.Duration

	// LeaseTTL in seconds (default 10)
	// For testing, can be set to 1-2 seconds to speed up lease expiration
	LeaseTTL int64

	// SuspectGrace is how long a route whose loss could not be attributed to its
	// owner stays resolvable, deprioritized, before it is dropped. Default 30s,
	// jittered by up to 20% per route.
	SuspectGrace time.Duration

	// SweepInterval is the resolution of the suspicion clock, default 1s. It
	// only advances while the watch is established.
	SweepInterval time.Duration
}

func Create(options Options) (gen.Registrar, error) {
	if len(options.Endpoints) == 0 {
		options.Endpoints = defaultEndpoins
	}

	if options.Cluster == "" {
		options.Cluster = "default"
	}

	if options.DialTimeout == 0 {
		options.DialTimeout = defaultDialTimeout
	}

	if options.RequestTimeout == 0 {
		options.RequestTimeout = defaultRequestTimeout
	}

	if options.LeaseTTL == 0 {
		options.LeaseTTL = 10 // default 10 seconds
	}

	if options.SuspectGrace == 0 {
		options.SuspectGrace = defaultSuspectGrace
	}

	if options.SweepInterval == 0 {
		options.SweepInterval = defaultSweepInterval
	}

	if options.KeepAlive == 0 {
		// Derive from LeaseTTL for fast dead connection detection.
		// Detection time = DialKeepAliveTime + DialKeepAliveTimeout = 2 * KeepAlive.
		// Must be less than LeaseTTL to allow endpoint failover before lease expires.
		options.KeepAlive = time.Duration(options.LeaseTTL) * time.Second / 3
		if options.KeepAlive < time.Second {
			options.KeepAlive = time.Second
		}
	}

	// Build etcd client configuration
	etcdloglevel := zap.NewAtomicLevelAt(zap.ErrorLevel)
	config := etcdcli.Config{
		Endpoints:            options.Endpoints,
		DialTimeout:          options.DialTimeout,
		DialKeepAliveTimeout: options.KeepAlive,
		DialKeepAliveTime:    options.KeepAlive,
		MaxCallSendMsgSize:   2 * 1024 * 1024, // 2MB
		MaxCallRecvMsgSize:   4 * 1024 * 1024, // 4MB
		RejectOldCluster:     true,
		LogConfig: &zap.Config{
			Level:         etcdloglevel,
			Encoding:      "json",
			EncoderConfig: zap.NewProductionEncoderConfig(),
		},
	}

	// Configure authentication
	if options.Username != "" && options.Password != "" {
		config.Username = options.Username
		config.Password = options.Password
	}

	// Configure TLS
	if options.TLS != nil {
		config.TLS = options.TLS
	} else if options.InsecureSkipVerify {
		config.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	cli, err := etcdcli.New(config)
	if err != nil {
		if errors.Is(err, etcdcli.ErrOldCluster) {
			// The client supports one minor back: this needs a server upgrade.
			return nil, fmt.Errorf("etcd server is too old for client %s, it needs etcd 3.6 or newer: %w",
				etcdversion.Version, err)
		}
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &client{
		options:           options,
		cli:               cli,
		leaseTTL:          options.LeaseTTL,
		ctx:               ctx,
		cancel:            cancel,
		pathCluster:       fmt.Sprintf(formatPathCluster, options.Cluster),
		pathClusterRoutes: fmt.Sprintf(formatPathClusterRoutes, options.Cluster),
		pathNodes:         fmt.Sprintf(formatPathNodes, options.Cluster),
		pathApps:          fmt.Sprintf(formatPathApps, options.Cluster),
		pathLeaving:       fmt.Sprintf(formatPathLeaving, options.Cluster),
		pathConfig:        fmt.Sprintf(formatPathConfig, options.Cluster),
		pathGlobalConfig:  formatPathGlobalConfig,
		config:            make(map[string]any),
		mirror:            newMirror(),
	}

	return client, nil
}

// Resolution priority (highest to lowest): 1 -> 2 -> 3 -> 4
//
// Example Usage:
// To set database.host for web1 node in production cluster:
//   etcdctl put services/ergo/cluster/production/config/web1/database.host "db.prod.com"
//
// To set default log level for all nodes in production:
//   etcdctl put services/ergo/cluster/production/config/*/log.level "info"
//
// To set cross-cluster setting for web1 in production:
//   etcdctl put services/ergo/config/production/web1/cache.size "256MB"
//
// To set global debug flag:
//   etcdctl put services/ergo/config/global/debug.enabled "false"
