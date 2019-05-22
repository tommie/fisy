package transfer

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

type filePairPDFSData struct {
	stack  []*filePair
	fun    func(context.Context, *filePair) ([]*filePair, error)
	nact   int
	failed bool
	c      *sync.Cond
}

func (dfs *filePairPDFSData) take() *filePair {
	dfs.c.L.Lock()
	defer dfs.c.L.Unlock()

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

func (dfs *filePairPDFSData) give(fps []*filePair) {
	dfs.c.L.Lock()
	defer dfs.c.L.Unlock()

	dfs.stack = append(dfs.stack, fps...)
	dfs.nact--

	if len(dfs.stack) == len(fps) {
		dfs.c.Broadcast()
	}
}

func (dfs *filePairPDFSData) fail() {
	dfs.c.L.Lock()
	defer dfs.c.L.Unlock()

	dfs.nact--

	if !dfs.failed {
		dfs.failed = true
		dfs.c.Broadcast()
	}
}

func (dfs *filePairPDFSData) loop(ctx context.Context) error {
	for {
		fp := dfs.take()
		if fp == nil {
			return nil
		}
		fps, err := dfs.fun(ctx, fp)
		if err != nil {
			dfs.fail()
			return err
		}
		dfs.give(fps)
	}
}

func filePairPDFS(ctx context.Context, roots []*filePair, fun func(context.Context, *filePair) ([]*filePair, error), nconc int) error {
	dfs := filePairPDFSData{
		stack: append([]*filePair{}, roots...),
		fun:   fun,
		c:     sync.NewCond(&sync.Mutex{}),
	}

	eg, cctx := errgroup.WithContext(ctx)
	for i := 0; i < nconc; i++ {
		eg.Go(func() error {
			return dfs.loop(cctx)
		})
	}
	return eg.Wait()
}
