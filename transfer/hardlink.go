package transfer

import (
	"sync"
	"syscall"

	"github.com/tommie/fisy/fs"
)

type linkSet struct {
	inodes map[uint64]*inodeInfo
	c      *sync.Cond
}

type inodeInfo struct {
	path     fs.Path
	uploaded bool
	nlink    int
}

func newLinkSet() linkSet {
	return linkSet{
		inodes: map[uint64]*inodeInfo{},
		c:      sync.NewCond(&sync.Mutex{}),
	}
}

func (set *linkSet) Offer(fp *filePair) {
	if fp.src == nil || fp.src.IsDir() {
		return
	}

	st, ok := fp.src.Sys().(*syscall.Stat_t)
	if !ok || st.Nlink < 2 {
		return
	}

	fp.linkToInode = st.Ino

	set.c.L.Lock()
	defer set.c.L.Unlock()

	if _, ok := set.inodes[st.Ino]; !ok {
		set.inodes[st.Ino] = &inodeInfo{
			path:  fp.path,
			nlink: int(st.Nlink),
		}
	}
}

func (set *linkSet) Len() int {
	set.c.L.Lock()
	defer set.c.L.Unlock()
	return len(set.inodes)
}

func (set *linkSet) Fulfill(fp *filePair) {
	set.c.L.Lock()
	defer set.c.L.Unlock()

	// If we failed to upload, this will cause other transfers to
	// fail as well.
	set.inodes[fp.linkToInode].uploaded = true
	set.inodes[fp.linkToInode].nlink--
	set.c.Broadcast()
}

func (set *linkSet) FinishedLinkPath(fp *filePair) fs.Path {
	set.c.L.Lock()
	defer set.c.L.Unlock()

	firstPath := set.inodes[fp.linkToInode].path
	if firstPath == fp.path {
		return ""
	}

	// We should hardlink. It is safe to block here since we know
	// firstPath was transferred before us.
	for !set.inodes[fp.linkToInode].uploaded {
		set.c.Wait()
	}

	set.inodes[fp.linkToInode].nlink--
	if set.inodes[fp.linkToInode].nlink == 0 {
		// Clean up. We don't need this in memory anymore.
		delete(set.inodes, fp.linkToInode)
	}

	return firstPath
}