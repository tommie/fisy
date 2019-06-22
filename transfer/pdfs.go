package transfer

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

// failPairPDFSData carries information about a parallel depth first
// search over filePairs.
type filePairPDFSData struct {
	stack  []*filePair
	fun    func(context.Context, *filePair) ([]*filePair, error)
	nact   int
	failed bool
	c      *sync.Cond
}

// take pops the next filePair and returns it. Returns nil if the
// traversal should stop. It also marks the filePair as
// in-progress. Either give or fail must be called exactly once after
// this function has completed successfully.
func (dfs *filePairPDFSData) take() *filePair {
	dfs.c.L.Lock()
	defer dfs.c.L.Unlock()

	// Even if the stack is empty, we need to wait until other
	// goroutines have stopped being active, since they may be
	// giving more work.
	for len(dfs.stack) == 0 && dfs.nact > 0 && !dfs.failed {
		dfs.c.Wait()
	}

	if dfs.failed {
		return nil
	}

	if len(dfs.stack) == 0 {
		return nil
	}

	n := len(dfs.stack)
	ret := dfs.stack[n-1]
	dfs.stack = dfs.stack[:n-1]
	dfs.nact++
	return ret
}

// give pushes some filePairs onto the DFS stack and releases the
// previous filePair.
func (dfs *filePairPDFSData) give(fps []*filePair) {
	dfs.c.L.Lock()
	defer dfs.c.L.Unlock()

	dfs.stack = append(dfs.stack, fps...)
	dfs.nact--

	if len(dfs.stack) == len(fps) {
		dfs.c.Broadcast()
	}
}

// fail marks the DFS as a failure, and it will stop traversing as
// soon as possible. It releases the previous filePair.
func (dfs *filePairPDFSData) fail() {
	dfs.c.L.Lock()
	defer dfs.c.L.Unlock()

	dfs.nact--

	if !dfs.failed {
		dfs.failed = true
		dfs.c.Broadcast()
	}
}

// loop runs the traversal. The caller can run these in parallel to
// speed up the traversal.
func (dfs *filePairPDFSData) loop(ctx context.Context) error {
	for {
		fp := dfs.take()
		if fp == nil {
			return ctx.Err()
		}
		fps, err := dfs.fun(ctx, fp)
		if err != nil {
			dfs.fail()
			return err
		}
		dfs.give(fps)
	}
}

// filePairPDFS runs a parallel depth-first traversal over a set of
// roots. The function performs actions and returns the children to be
// processed next. There are no guarantees that deep files are
// processed first, so fun should make sure to do all preparations
// that are needed for the processing of the returned children to
// succeed. nconc determines how many parallel invocations of fun are
// allowed.
func filePairPDFS(ctx context.Context, roots []*filePair, fun func(context.Context, *filePair) ([]*filePair, error), nconc int) error {
	dfs := filePairPDFSData{
		stack: append([]*filePair{}, roots...),
		fun:   fun,
		c:     sync.NewCond(&sync.Mutex{}),
	}

	eg, cctx := errgroup.WithContext(ctx)

	go func() {
		<-cctx.Done()

		dfs.c.L.Lock()
		defer dfs.c.L.Unlock()

		dfs.failed = true
		dfs.c.Broadcast()
	}()

	for i := 0; i < nconc; i++ {
		eg.Go(func() error {
			return dfs.loop(cctx)
		})
	}
	return eg.Wait()
}
