package transfer

import (
	"sync"
	"syscall"

	"github.com/tommie/fisy/fs"
)

// linkSet contains a map of source inode information. It is used to
// ensure only one goroutine creates an inode, and other goroutines
// are blocking until it's done.
type linkSet struct {
	inodes map[uint64]*inodeInfo
	c      *sync.Cond
}

// An inodeInfo contains information about a single source inode.
type inodeInfo struct {
	path     fs.Path
	uploaded bool
	nlink    int
}

// newLinkSet creates a new, empty, link set.
func newLinkSet() linkSet {
	return linkSet{
		inodes: map[uint64]*inodeInfo{},
		c:      sync.NewCond(&sync.Mutex{}),
	}
}

// Offer tells the link set that this file exists. The filePair is
// updated with linkToInode. The file is recorded as interesting if it
// has more than one link.
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
			nlink: int(st.Nlink),
		}
	}
}

// Len returns the current size of the set.
func (set *linkSet) Len() int {
	set.c.L.Lock()
	defer set.c.L.Unlock()
	return len(set.inodes)
}

// Fulfill informs the link set that the destination file is now ready.
func (set *linkSet) Fulfill(fp *filePair) {
	set.c.L.Lock()
	defer set.c.L.Unlock()

	// If we failed to upload, this will cause other transfers to
	// fail as well.
	set.inodes[fp.linkToInode].uploaded = true
	set.inodes[fp.linkToInode].nlink--
	set.c.Broadcast()
}

// Discard removes a file from the set. Use this if the initial file
// transfer failed. This lets another goroutine take over transfer.
func (set *linkSet) Discard(fp *filePair) {
	set.c.L.Lock()
	defer set.c.L.Unlock()

	if set.inodes[fp.linkToInode].path == fp.path {
		set.inodes[fp.linkToInode].path = ""
	}
	set.c.Broadcast()
}

// FinishedLinkPath returns the path of a finished destination
// file. It blocks until the file is ready. If this returns empty, it
// means the source file must be transferred.
func (set *linkSet) FinishedLinkPath(fp *filePair) fs.Path {
	set.c.L.Lock()
	defer set.c.L.Unlock()

	for !set.inodes[fp.linkToInode].uploaded {
		if set.inodes[fp.linkToInode].path == "" {
			// We are the first one here, or the previous
			// file was discarded.
			set.inodes[fp.linkToInode].path = fp.path
			return ""
		}
		set.c.Wait()
	}

	firstPath := set.inodes[fp.linkToInode].path

	set.inodes[fp.linkToInode].nlink--
	if set.inodes[fp.linkToInode].nlink == 0 {
		// Clean up. We don't need this in memory anymore.
		delete(set.inodes, fp.linkToInode)
	}

	return firstPath
}
