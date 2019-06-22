package transfer

import (
	"os"
	"sync"

	"github.com/tommie/fisy/fs"
)

// linkSet contains a map of source inode information. It is used to
// ensure only one goroutine creates an inode, and other goroutines
// are blocking until it's done.
//
// Example:
//
//   ls := newLinkSet()
//   inode, targetPath := ls.FinishedFile(destPath, srcFileInfo)
//   if inode != 0 {
//     if targetPath != "" {
//       return os.Link(targetPath, destPath)
//     }
//     if err := transferFile(srcFileInfo, destPath); err != nil {
//       ls.Discard(inode, destPath)
//       return err
//     }
//     ls.Fulfill(inode, destPath)
//   }
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

// Len returns the current size of the set.
func (set *linkSet) Len() int {
	set.c.L.Lock()
	defer set.c.L.Unlock()
	return len(set.inodes)
}

// Fulfill informs the link set that the destination file is now ready.
func (set *linkSet) Fulfill(inode uint64) {
	set.c.L.Lock()
	defer set.c.L.Unlock()

	// If we failed to upload, this will cause other transfers to
	// fail as well.
	set.inodes[inode].uploaded = true
	set.decrementLink(inode)
	set.c.Broadcast()
}

// decrementLink decrements the link count for the inode, and removes
// the inode info if there are no more links.
func (set *linkSet) decrementLink(inode uint64) {
	set.inodes[inode].nlink--
	if set.inodes[inode].nlink == 0 {
		// Clean up. We don't need this in memory anymore.
		delete(set.inodes, inode)
	}

}

// Discard removes a file from the set. Use this if the initial file
// transfer failed. This lets another goroutine take over transfer.
func (set *linkSet) Discard(inode uint64, path fs.Path) {
	set.c.L.Lock()
	defer set.c.L.Unlock()

	if set.inodes[inode].path == path {
		set.inodes[inode].path = ""
	}
	set.decrementLink(inode)
	set.c.Broadcast()
}

// FinishedFile returns the inode and path of a finished destination
// file. It blocks until the file is ready. If this returns zero, it
// means the inode has no other links. If this returns the empty
// string, it means the source file must be transferred.
func (set *linkSet) FinishedFile(path fs.Path, src os.FileInfo) (uint64, fs.Path) {
	set.c.L.Lock()
	defer set.c.L.Unlock()

	inode := set.offerLocked(src)
	if inode == 0 {
		return 0, ""
	}

	for !set.inodes[inode].uploaded {
		if set.inodes[inode].path == "" {
			// We are the first one here, or the previous
			// file was discarded.
			set.inodes[inode].path = path
			return inode, ""
		}
		set.c.Wait()
	}

	firstPath := set.inodes[inode].path
	set.decrementLink(inode)

	return inode, firstPath
}

// offerLocked tells the link set that this file exists. The file is
// recorded as interesting if it has more than one link. The inode of
// the source file is returned if interesting. Otherwise zero is
// returned.
func (set *linkSet) offerLocked(src os.FileInfo) uint64 {
	if src == nil || src.IsDir() {
		return 0
	}

	attrs, ok := fs.FileAttrsFromFileInfo(src)
	if !ok || attrs.NLinks < 2 {
		return 0
	}

	if _, ok := set.inodes[attrs.Inode]; !ok {
		set.inodes[attrs.Inode] = &inodeInfo{
			nlink: int(attrs.NLinks),
		}
	}

	return attrs.Inode
}
