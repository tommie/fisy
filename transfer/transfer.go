package transfer

import (
	"errors"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/tommie/fisy/fs"
)

// A FileOperation describes one of the transfer file operations.
type FileOperation rune

const (
	UnknownFileOperation FileOperation = '?'
	Create               FileOperation = 'C'
	Remove               FileOperation = 'R'
	Keep                 FileOperation = 'K'
	Update               FileOperation = 'U'
)

// A FileHook is a function that is called with updates about a file
// transfer. uploadedBytes must be accessed using atomic.LoadUint64,
// if done outside the hook function.
type FileHook func(fi os.FileInfo, op FileOperation, uploadedBytes *uint64, err error)

// InProgress indicates that the file is being transferred. It's a
// temporary condition, not an error.
var InProgress = errors.New("in progress")

// commonModeMask is the non-special mode bits to transfer. Doesn't
// include file type bits.
const commonModeMask os.FileMode = os.ModePerm

// A failPair describes a file in a transfer operation. The path
// identifies the file on both sides. src is nil if this is file has
// been removed, and dest is nil if the file didn't exist before.
type filePair struct {
	path fs.Path
	src  os.FileInfo
	dest os.FileInfo
}

// FileInfo returns overall file information about the file.
func (fp *filePair) FileInfo() os.FileInfo {
	if fp.src != nil {
		return &filePairInfo{fp.src, fp.path}
	}
	return &filePairInfo{fp.dest, fp.path}
}

// A filePairInfo is a FileInfo for a filePair. It overrides the name
// to be the full relative path.
type filePairInfo struct {
	os.FileInfo
	path fs.Path
}

func (fi *filePairInfo) Name() string { return string(fi.path) }

// FileOperation returns the type of operation this file pair needs to
// synchronize.
func (fp *filePair) FileOperation() FileOperation {
	switch {
	case fp.src != nil && fp.dest != nil:
		if fp.src.Mode().IsDir() {
			if directoryNeedsTransfer(fp.dest, fp.src) {
				return Update
			} else {
				return Keep
			}
		} else {
			if fileNeedsTransfer(fp.dest, fp.src) {
				return Update
			} else {
				return Keep
			}
		}
	case fp.src != nil:
		return Create
	case fp.dest != nil:
		return Remove
	default:
		glog.Fatalf("unknown file operation for %+v", fp)
		panic(nil)
	}
}

// fileNeedsTransfer returns true if the source and destination as different.
func fileNeedsTransfer(dest, src os.FileInfo) bool {
	if dest == nil {
		return true
	}
	md := dest.ModTime().Sub(src.ModTime())
	if md < 0 {
		md = -md
	}
	return dest.Size() != src.Size() ||
		dest.Mode()&commonModeMask != src.Mode()&commonModeMask ||
		md > 1*time.Second
}

// directoryNeedsTransfer returns true if the source and destination as different.
func directoryNeedsTransfer(dest, src os.FileInfo) bool {
	// We force u+w so we can continue working on the directory.
	return dest == nil || dest.Mode()&commonModeMask&^0200 != src.Mode()&commonModeMask&^0200
}
