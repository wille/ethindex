package indexer

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// fakeChain fabricates deterministic header chains for tests. Hashes
// encode (fork, number) so distinct forks never collide.
type fakeChain struct {
	fork byte
}

func (f fakeChain) hash(number uint64) common.Hash {
	var h common.Hash
	h[0] = f.fork
	copy(h[24:], big.NewInt(int64(number)).FillBytes(make([]byte, 8)))
	return h
}

func (f fakeChain) ref(number uint64) headRef {
	return headRef{Number: number, Hash: f.hash(number), ParentHash: f.hash(number - 1)}
}

// header builds a *types.Header whose ParentHash matches the fake chain
// linkage. Note types.Header.Hash() is the real RLP hash, so classify
// tests only rely on Number and ParentHash comparisons.
func (f fakeChain) header(number uint64) *types.Header {
	return &types.Header{Number: new(big.Int).SetUint64(number), ParentHash: f.hash(number - 1)}
}

func seed(c *chainState, f fakeChain, from, to uint64) {
	for n := from; n <= to; n++ {
		c.record(f.ref(n))
	}
}

func TestClassifyFirstHead(t *testing.T) {
	c := newChainState(12)
	if got := c.classify(fakeChain{1}.header(100)); got != headExtend {
		t.Errorf("first head = %v, want extend", got)
	}
}

func TestClassifyExtend(t *testing.T) {
	f := fakeChain{1}
	c := newChainState(12)
	seed(c, f, 100, 110)
	if got := c.classify(f.header(111)); got != headExtend {
		t.Errorf("classify = %v, want extend", got)
	}
}

func TestClassifyGap(t *testing.T) {
	f := fakeChain{1}
	c := newChainState(12)
	seed(c, f, 100, 110)
	if got := c.classify(f.header(115)); got != headGap {
		t.Errorf("classify = %v, want gap", got)
	}
}

func TestClassifyReorgSameHeight(t *testing.T) {
	f, g := fakeChain{1}, fakeChain{2}
	c := newChainState(12)
	seed(c, f, 100, 110)
	// Competing head at tip height with a different parent.
	if got := c.classify(g.header(110)); got != headReorg {
		t.Errorf("classify = %v, want reorg", got)
	}
	// Next head building on an unknown parent.
	if got := c.classify(g.header(111)); got != headReorg {
		t.Errorf("classify = %v, want reorg", got)
	}
}

func TestFindAncestor(t *testing.T) {
	f, g := fakeChain{1}, fakeChain{2}
	c := newChainState(12)
	seed(c, f, 100, 110)

	// Canonical chain (g) diverges after block 107: g agrees with our
	// buffer up to 107 (simulate by making g return f's refs <= 107).
	canonical := forkAt{fakeOld: f, fakeNew: g, forkAfter: 107}
	ancestor, deep, err := c.findAncestor(canonical, 110)
	if err != nil {
		t.Fatal(err)
	}
	if deep {
		t.Error("unexpected deep reorg")
	}
	if ancestor != 107 {
		t.Errorf("ancestor = %d, want 107", ancestor)
	}
}

// TestFindAncestorSparseRing: the ring's computed floor can lie below
// its oldest actual entry (a chain tracked from mid-window). The match
// then sits in a final PARTIAL chunk whose flush must not be skipped -
// the regression here made every such reorg look deeper than the
// buffer, triggering rescans of blocks that predate the process.
func TestFindAncestorSparseRing(t *testing.T) {
	f, g := fakeChain{1}, fakeChain{2}
	c := newChainState(1)  // depth 64
	seed(c, f, 1500, 1520) // entries 1500..1520 only; floor = 1457

	// Fork diverges after 1500; walk from 1510 ends its last chunk on
	// ring numbers (1502..1500) followed by unbuffered ones.
	ancestor, deep, err := c.findAncestor(forkAt{fakeOld: f, fakeNew: g, forkAfter: 1500}, 1510)
	if err != nil {
		t.Fatal(err)
	}
	if deep {
		t.Error("unexpected deep reorg")
	}
	if ancestor != 1500 {
		t.Errorf("ancestor = %d, want 1500", ancestor)
	}
}

func TestFindAncestorDeeperThanBuffer(t *testing.T) {
	f, g := fakeChain{1}, fakeChain{2}
	c := newChainState(1) // depth 64
	seed(c, f, 1000, 1100)

	// The entire buffered range disagrees.
	ancestor, deep, err := c.findAncestor(forkAt{fakeOld: f, fakeNew: g, forkAfter: 0}, 1100)
	if err != nil {
		t.Fatal(err)
	}
	if !deep {
		t.Error("expected deep reorg")
	}
	if want := c.floor(); ancestor != want {
		t.Errorf("ancestor = %d, want floor %d", ancestor, want)
	}
}

// forkAt serves old-chain refs up to forkAfter and new-chain refs above.
type forkAt struct {
	fakeOld, fakeNew fakeChain
	forkAfter        uint64
}

func (f forkAt) headerRefsByNumbers(numbers []uint64) ([]headRef, []error, error) {
	refs := make([]headRef, len(numbers))
	for i, n := range numbers {
		if n <= f.forkAfter {
			refs[i] = f.fakeOld.ref(n)
		} else {
			refs[i] = f.fakeNew.ref(n)
		}
	}
	return refs, make([]error, len(numbers)), nil
}

func TestResetTipAfterLowerReorg(t *testing.T) {
	f, g := fakeChain{1}, fakeChain{2}
	c := newChainState(12)
	seed(c, f, 100, 120) // tip 120 on chain f

	// The chain reorgs onto fork g whose head is LOWER (110): the
	// rescan re-records 101-110 on the fork, then the tip is reset.
	seed(c, g, 101, 110)
	c.resetTip(110)

	if c.tip.Number != 110 || c.tip.Hash != g.hash(110) {
		t.Fatalf("tip = %+v, want fork block 110", c.tip)
	}
	// The next fork head extends cleanly instead of looking like yet
	// another reorg against the orphaned old tip.
	if got := c.classify(g.header(111)); got != headExtend {
		t.Errorf("classify(111') = %v, want extend", got)
	}
}

func TestRingBufferEviction(t *testing.T) {
	f := fakeChain{1}
	c := newChainState(12) // depth 64
	seed(c, f, 0, 200)

	if _, ok := c.at(200); !ok {
		t.Error("tip should be buffered")
	}
	if _, ok := c.at(200 - 63); !ok {
		t.Error("floor should be buffered")
	}
	if _, ok := c.at(100); ok {
		t.Error("evicted block should not be reported as buffered")
	}
	if got := c.floor(); got != 200-63 {
		t.Errorf("floor = %d", got)
	}
}

func TestDepthScalesWithConfirmations(t *testing.T) {
	if got := newChainState(100).depth; got != 116 {
		t.Errorf("depth = %d, want 116", got)
	}
	if got := newChainState(5).depth; got != 64 {
		t.Errorf("depth = %d, want 64", got)
	}
}
