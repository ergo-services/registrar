package etcd

import (
	"sync"
	"testing"

	"ergo.services/ergo/gen"
)

// drive runs n rotations against entry/participants and returns the sequence
// of chosen winner node names.
func drive(t *testing.T, entry *appEntry, participants []gen.ApplicationRoute, n int) []gen.Atom {
	t.Helper()
	out := make([]gen.Atom, 0, n)
	for i := 0; i < n; i++ {
		result := rotateAppRoutes(entry, participants, entry.rrGen)
		if len(result) != len(participants) {
			t.Fatalf("rotation lost routes: got %d, want %d", len(result), len(participants))
		}
		out = append(out, result[0].Node)
	}
	return out
}

func TestRotateSmoothWRR(t *testing.T) {
	participants := []gen.ApplicationRoute{
		{Node: "a@h", Weight: 5},
		{Node: "b@h", Weight: 1},
		{Node: "c@h", Weight: 1},
	}
	entry := &appEntry{rrGen: 1}

	got := drive(t, entry, participants, 7)
	want := []gen.Atom{"a@h", "a@h", "b@h", "a@h", "c@h", "a@h", "a@h"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("step %d: got %s, want %s (full sequence %v)", i, got[i], want[i], got)
		}
	}
}

func TestRotateEqualWeights(t *testing.T) {
	participants := []gen.ApplicationRoute{
		{Node: "a@h", Weight: 1},
		{Node: "b@h", Weight: 1},
		{Node: "c@h", Weight: 1},
	}
	entry := &appEntry{rrGen: 1}

	got := drive(t, entry, participants, 9)
	counts := map[gen.Atom]int{}
	for _, n := range got {
		counts[n]++
	}
	for _, p := range participants {
		if counts[p.Node] != 3 {
			t.Fatalf("node %s appeared %d times, want 3 (sequence %v)", p.Node, counts[p.Node], got)
		}
	}
}

func TestRotateWeightZeroNormalized(t *testing.T) {
	participants := []gen.ApplicationRoute{
		{Node: "a@h", Weight: 0},
		{Node: "b@h", Weight: 0},
	}
	entry := &appEntry{rrGen: 1}

	got := drive(t, entry, participants, 6)
	counts := map[gen.Atom]int{}
	for _, n := range got {
		counts[n]++
	}
	if counts["a@h"] != 3 || counts["b@h"] != 3 {
		t.Fatalf("expected 3/3 split, got %v", counts)
	}
}

func TestRotateSelfIncluded(t *testing.T) {
	// A "self" node has no special treatment — it participates with its
	// declared weight just like any other.
	participants := []gen.ApplicationRoute{
		{Node: "self@h", Weight: 3},
		{Node: "peer@h", Weight: 1},
	}
	entry := &appEntry{rrGen: 1}

	got := drive(t, entry, participants, 4)
	counts := map[gen.Atom]int{}
	for _, n := range got {
		counts[n]++
	}
	if counts["self@h"] != 3 || counts["peer@h"] != 1 {
		t.Fatalf("expected self:3 peer:1, got %v (sequence %v)", counts, got)
	}
}

func TestRotateTailOrder(t *testing.T) {
	// After the first WRR step on [5,1,1], current_weight is {A:-2, B:1, C:1}.
	// Winner A goes to [0]; tail must be ordered by cw desc then name asc:
	// B and C tie at cw=1, so B (alphabetical) precedes C.
	participants := []gen.ApplicationRoute{
		{Node: "a@h", Weight: 5},
		{Node: "b@h", Weight: 1},
		{Node: "c@h", Weight: 1},
	}
	entry := &appEntry{rrGen: 1}

	result := rotateAppRoutes(entry, participants, entry.rrGen)
	if result[0].Node != "a@h" || result[1].Node != "b@h" || result[2].Node != "c@h" {
		t.Fatalf("unexpected order: %v", []gen.Atom{result[0].Node, result[1].Node, result[2].Node})
	}
}

func TestRotateRebuildOnGenBump(t *testing.T) {
	// First rotation with one set of participants seeds rrState. Bumping
	// rrGen (as a Watch event would) forces a rebuild on the next call so
	// stale current_weights don't pollute the new participant set.
	entry := &appEntry{rrGen: 1}
	first := []gen.ApplicationRoute{
		{Node: "a@h", Weight: 5},
		{Node: "b@h", Weight: 1},
	}
	rotateAppRoutes(entry, first, entry.rrGen)
	rotateAppRoutes(entry, first, entry.rrGen)
	// rrState now has skew toward A.

	entry.rrGen++ // simulate Watch-driven mutation
	second := []gen.ApplicationRoute{
		{Node: "x@h", Weight: 1},
		{Node: "y@h", Weight: 1},
	}
	got := drive(t, entry, second, 4)
	counts := map[gen.Atom]int{}
	for _, n := range got {
		counts[n]++
	}
	if counts["x@h"] != 2 || counts["y@h"] != 2 {
		t.Fatalf("expected fresh rotation after rrGen bump, got %v", counts)
	}
}

func TestRotateConcurrent(t *testing.T) {
	// Goal: -race must stay clean and all calls must produce a result whose
	// total length equals participants. The frequency distribution should
	// roughly match the weights — exact RR ordering is not preserved when
	// many goroutines interleave through rrLock.
	participants := []gen.ApplicationRoute{
		{Node: "a@h", Weight: 2},
		{Node: "b@h", Weight: 1},
		{Node: "c@h", Weight: 1},
	}
	entry := &appEntry{rrGen: 1}

	const goroutines = 50
	const perG = 200
	total := goroutines * perG

	var mu sync.Mutex
	counts := map[gen.Atom]int{}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				result := rotateAppRoutes(entry, participants, entry.rrGen)
				if len(result) != len(participants) {
					t.Errorf("lost routes: got %d", len(result))
					return
				}
				mu.Lock()
				counts[result[0].Node]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Expected: A ~ total/2, B ~ total/4, C ~ total/4. Allow generous slack
	// because contention on rrLock can shift the order of steps slightly
	// but cannot break the smooth-WRR invariant: over N steps each node n
	// gets exactly N * w_n / sum(w) wins (with at most one unit of skew).
	wantA := total / 2
	wantB := total / 4
	wantC := total / 4
	if counts["a@h"] != wantA || counts["b@h"] != wantB || counts["c@h"] != wantC {
		t.Fatalf("frequency mismatch: got %v, want a:%d b:%d c:%d",
			counts, wantA, wantB, wantC)
	}
}

func TestSplitAppRoutesIncludesAll(t *testing.T) {
	// Self-fairness check at the snapshot layer: the snapshot must include
	// every node in the routes map without filtering self. The etcd client
	// no longer hides the local node; rotation handles it as a peer.
	entry := &appEntry{
		routes: map[gen.Atom]gen.ApplicationRoute{
			"self@h": {Node: "self@h", Weight: 1},
			"peer@h": {Node: "peer@h", Weight: 1},
		},
	}
	healthy, suspect := splitAppRoutes(entry)
	if len(healthy) != 2 || len(suspect) != 0 {
		t.Fatalf("split: got %d healthy and %d suspect, want 2 and 0", len(healthy), len(suspect))
	}
	if healthy[0].Node != "peer@h" || healthy[1].Node != "self@h" {
		t.Fatalf("snapshot not name-sorted: %v %v", healthy[0].Node, healthy[1].Node)
	}
}

func TestMirrorPutBumpsRrGen(t *testing.T) {
	m := newMirror()
	m.putAppRoute("myapp", "a@h", gen.ApplicationRoute{Node: "a@h", Name: "myapp", Weight: 1}, 1)
	entry := m.apps["myapp"]

	if entry.rrGen != 1 {
		t.Fatalf("rrGen after Put: got %d, want 1", entry.rrGen)
	}
	if _, ok := entry.routes["a@h"]; ok == false {
		t.Fatalf("route a@h not added")
	}
}

func TestMirrorRemoveBumpsRrGen(t *testing.T) {
	m := newMirror()
	m.putAppRoute("myapp", "a@h", gen.ApplicationRoute{Node: "a@h", Name: "myapp", Weight: 1}, 1)
	m.putAppRoute("myapp", "b@h", gen.ApplicationRoute{Node: "b@h", Name: "myapp", Weight: 1}, 1)
	entry := m.apps["myapp"]
	before := entry.rrGen

	m.removeAppRoute("myapp", "a@h", 2)

	if entry.rrGen != before+1 {
		t.Fatalf("rrGen after remove: got %d, want %d", entry.rrGen, before+1)
	}
	if _, ok := entry.routes["a@h"]; ok {
		t.Fatalf("route a@h not removed")
	}
}

func TestMirrorStaleRevIgnored(t *testing.T) {
	// Watch events older than entry.rev must be ignored entirely, neither
	// routes nor rrGen may change. Otherwise a late delivery could trigger
	// spurious rebuilds and quietly corrupt the mirror.
	m := newMirror()
	m.putAppRoute("myapp", "a@h", gen.ApplicationRoute{Node: "a@h", Name: "myapp", Weight: 1}, 10)
	entry := m.apps["myapp"]
	rrGen := entry.rrGen

	m.putAppRoute("myapp", "b@h", gen.ApplicationRoute{Node: "b@h", Name: "myapp", Weight: 1}, 5)
	m.removeAppRoute("myapp", "a@h", 5)

	if entry.rrGen != rrGen {
		t.Fatalf("rrGen bumped on stale rev: got %d, want %d", entry.rrGen, rrGen)
	}
	if _, ok := entry.routes["b@h"]; ok {
		t.Fatalf("stale Put applied")
	}
	if _, ok := entry.routes["a@h"]; ok == false {
		t.Fatalf("stale remove applied")
	}
}

func TestRotationAfterWatchPutIncludesNewNode(t *testing.T) {
	// Warm rotation on a two-node set so rrState carries skew, then deliver
	// a Watch-Put for a third node. The post-Put rotation must include the
	// newcomer with a fair share, proving that rrGen bump leads the next
	// wrrStep to rebuild rather than carry old current_weights.
	m := newMirror()
	m.putAppRoute("myapp", "a@h", gen.ApplicationRoute{Node: "a@h", Name: "myapp", Weight: 1}, 1)
	m.putAppRoute("myapp", "b@h", gen.ApplicationRoute{Node: "b@h", Name: "myapp", Weight: 1}, 1)
	entry := m.apps["myapp"]

	for i := 0; i < 4; i++ {
		healthy, _ := splitAppRoutes(entry)
		rotateAppRoutes(entry, healthy, entry.rrGen)
	}

	m.putAppRoute("myapp", "c@h", gen.ApplicationRoute{Node: "c@h", Name: "myapp", Weight: 1}, 2)

	counts := map[gen.Atom]int{}
	for i := 0; i < 6; i++ {
		healthy, _ := splitAppRoutes(entry)
		result := rotateAppRoutes(entry, healthy, entry.rrGen)
		counts[result[0].Node]++
	}
	if counts["a@h"] != 2 || counts["b@h"] != 2 || counts["c@h"] != 2 {
		t.Fatalf("post-Put distribution: got %v, want 2/2/2", counts)
	}
}

func TestRotationAfterWatchDeleteSkipsNode(t *testing.T) {
	m := newMirror()
	for _, node := range []gen.Atom{"a@h", "b@h", "c@h"} {
		m.putAppRoute("myapp", node, gen.ApplicationRoute{Node: node, Name: "myapp", Weight: 1}, 1)
	}
	entry := m.apps["myapp"]

	for i := 0; i < 3; i++ {
		healthy, _ := splitAppRoutes(entry)
		rotateAppRoutes(entry, healthy, entry.rrGen)
	}

	m.removeAppRoute("myapp", "c@h", 2)

	counts := map[gen.Atom]int{}
	for i := 0; i < 4; i++ {
		healthy, _ := splitAppRoutes(entry)
		result := rotateAppRoutes(entry, healthy, entry.rrGen)
		counts[result[0].Node]++
	}
	if counts["c@h"] != 0 {
		t.Fatalf("deleted node still selected: %v", counts)
	}
	if counts["a@h"] != 2 || counts["b@h"] != 2 {
		t.Fatalf("post-Delete distribution: got %v, want a:2 b:2", counts)
	}
}

func TestRotationIgnoresLingeringRrStateKeys(t *testing.T) {
	// Hand-craft an entry where rrState carries a "zombie" key absent from
	// the current participant set, simulating what would happen if rebuild
	// somehow lagged. The algorithm reads rrState only via participant
	// names, so a zombie — even with a huge stale weight — must neither
	// win nor distort the legitimate distribution.
	entry := &appEntry{
		rrGen:  2,
		rrSeen: 2, // matches rrGen so wrrStep takes the no-rebuild branch
		rrState: map[gen.Atom]int{
			"zombie@h": 1000,
			"a@h":      0,
			"b@h":      0,
		},
	}
	participants := []gen.ApplicationRoute{
		{Node: "a@h", Weight: 1},
		{Node: "b@h", Weight: 1},
	}
	counts := map[gen.Atom]int{}
	for i := 0; i < 6; i++ {
		result := rotateAppRoutes(entry, participants, entry.rrGen)
		counts[result[0].Node]++
	}
	if _, ok := counts["zombie@h"]; ok {
		t.Fatalf("zombie selected as winner: %v", counts)
	}
	if counts["a@h"] != 3 || counts["b@h"] != 3 {
		t.Fatalf("zombie distorted rotation: got %v, want 3/3", counts)
	}
}
