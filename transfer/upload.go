package transfer

import (
	"context"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/tommie/fisy/fs"
	"github.com/tommie/fisy/remote"
)

// An Upload contains information about an in-progress upload. While
// Run is executing, Stats can be used to get progress information.
type Upload struct {
	process

	srcLinks linkSet

	stats UploadStats
}

// NewUpload creates a new upload, with the given destination and source.
func NewUpload(dest fs.WriteableFileSystem, src fs.ReadableFileSystem, opts ...UploadOpt) *Upload {
	u := &Upload{
		process: process{
			src:  src,
			dest: dest,
		},

		srcLinks: newLinkSet(),

		stats: UploadStats{
			lastPath: &atomic.Value{},
		},
	}
	u.process.stats = &u.stats.ProcessStats
	u.process.transfer = u.transfer
	for _, opt := range opts {
		opt(u)
	}
	return u
}

// An UploadOpt is an option to NewUpload.
type UploadOpt func(*Upload)

// WithIgnoreFilter adds a filter function. If the function returns
// true for a file or directory, it will be completely ignored.
func WithIgnoreFilter(fun func(fs.Path) bool) UploadOpt {
	return func(u *Upload) {
		u.ignoreFilter = fun
	}
}

// WithConcurrency sets the transfer concurrency, in files.
func WithConcurrency(nconc int) UploadOpt {
	if nconc < 1 {
		glog.Fatalf("nconc must be at least 1")
	}

	return func(u *Upload) {
		u.nconc = nconc
	}
}

// Transfer ensures a single file or directory has been fully
// transferred. It may do retries in case of failure.
func (u *Upload) transfer(ctx context.Context, fp *filePair) error {
	return remote.Idempotent(ctx, func() error {
		u.stats.lastPath.Store(fp)

		if fp.FileInfo().IsDir() {
			return u.transferDirectory(fp)
		}

		return u.transferFile(fp)
	})
}

// transferFile transfers a single file from source to dest.
func (u *Upload) transferFile(fp *filePair) error {
	if fp.src == nil {
		// Removed file.
		glog.V(1).Infof("Removing file %q...", fp.path)
		atomic.AddUint64(&u.stats.RemovedFiles, 1)
		return u.dest.Remove(fp.path)
	}

	inode, firstPath := u.srcLinks.FinishedFile(fp.path, fp.src)
	if inode != 0 {
		if firstPath != "" {
			glog.V(1).Infof("Hard-linking file %q to %q...", fp.path, firstPath)
			atomic.AddUint64(&u.stats.UploadedFiles, 1)
			return u.dest.Link(firstPath, fp.path)
		}

		defer u.srcLinks.Fulfill(inode)
	}

	if fp.dest != nil && !needsTransfer(fp.dest, fp.src) {
		glog.V(1).Infof("Keeping file %q...", fp.path)
		if err := u.dest.Keep(fp.path); err == nil {
			atomic.AddUint64(&u.stats.KeptBytes, uint64(fp.dest.Size()))
			atomic.AddUint64(&u.stats.KeptFiles, 1)
			return nil
		} else {
			glog.V(2).Infof("Failed to keep: %v", err)
		}

		// Fall back to normal transfer.
	}

	if fp.src.Mode()&os.ModeSymlink != 0 {
		return u.createSymlink(fp, inode)
	}

	return u.copyFile(fp, inode)
}

// needsTransfer returns true if the source and destination as different.
func needsTransfer(dest, src os.FileInfo) bool {
	if dest.IsDir() {
		// We force u+w so we can continue working on the directory.
		return dest.Mode()&commonModeMask&^0200 != src.Mode()&commonModeMask&^0200
	}
	return dest.Size() != src.Size() ||
		dest.Mode()&commonModeMask != src.Mode()&commonModeMask ||
		!dest.ModTime().Truncate(time.Second).Equal(src.ModTime().Truncate(time.Second))
}

func (u *Upload) createSymlink(fp *filePair, inode uint64) error {
	linkdest, err := u.src.Readlink(fp.path)
	if fs.IsNotExist(err) {
		// The symlink was removed between listing and transferring.
		u.srcLinks.Discard(inode, fp.path)
		atomic.AddUint64(&u.stats.DiscardedFiles, 1)
		return nil
	} else if err != nil {
		return err
	}
	glog.V(1).Infof("Symlinking %q to %q...", fp.path, linkdest)
	atomic.AddUint64(&u.stats.UploadedBytes, uint64(len(linkdest)))
	atomic.AddUint64(&u.stats.UploadedFiles, 1)
	return u.dest.Symlink(linkdest, fp.path)
}

// copyFile copies a file byte-by-byte.
func (u *Upload) copyFile(fp *filePair, inode uint64) error {
	sf, err := u.src.Open(fp.path)
	if fs.IsNotExist(err) {
		// The file was removed between listing and transferring.
		u.srcLinks.Discard(inode, fp.path)
		atomic.AddUint64(&u.stats.DiscardedFiles, 1)
		return nil
	} else if err != nil {
		return err
	}
	defer sf.Close()

	df, err := u.dest.Create(fp.path)
	if fs.IsPermission(err) {
		u.dest.Remove(fp.path)
		df, err = u.dest.Create(fp.path)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	atime := fp.src.ModTime()
	err = func() error {
		if err := df.Chmod(fp.src.Mode() & commonModeMask); err != nil {
			return err
		}

		glog.V(1).Infof("Uploading file %q (%d bytes)...", fp.path, fp.src.Size())
		_, err = io.Copy(df, sf)
		if err != nil {
			return err
		}

		if attrs, ok := fs.FileAttrsFromFileInfo(fp.src); ok {
			if err := df.Chown(attrs.UID, attrs.GID); err != nil {
				return err
			}
			atime = attrs.AccessTime
		}
		return nil
	}()
	if err != nil {
		df.Close()
		u.dest.Remove(fp.path)
		return err
	}

	if err := df.Close(); err != nil {
		u.dest.Remove(fp.path)
		return err
	}

	if err := u.dest.Chtimes(fp.path, atime, fp.src.ModTime()); err != nil {
		u.dest.Remove(fp.path)
		return err
	}

	atomic.AddUint64(&u.stats.UploadedBytes, uint64(fp.src.Size()))
	atomic.AddUint64(&u.stats.UploadedFiles, 1)

	return nil
}

// transferDirectory transfers a single directory.
func (u *Upload) transferDirectory(fp *filePair) error {
	if fp.src == nil {
		// Removed directory.
		glog.V(1).Infof("Removing directory %q...", fp.path)
		atomic.AddUint64(&u.stats.RemovedDirectories, 1)
		return u.dest.RemoveAll(fp.path)
	}

	if fp.dest != nil && !needsTransfer(fp.dest, fp.src) {
		glog.V(1).Infof("Keeping directory %q...", fp.path)
		if err := u.dest.Keep(fp.path); err == nil {
			atomic.AddUint64(&u.stats.KeptDirectories, 1)
			return nil
		} else {
			glog.V(2).Infof("Failed to keep: %v", err)
		}

		// Fall back to normal transfer.
	}

	return u.makeDirectory(fp)
}

func (u *Upload) makeDirectory(fp *filePair) error {
	attrs, ok := fs.FileAttrsFromFileInfo(fp.src)
	if !ok {
		attrs.UID = -1
		attrs.GID = -1
	}

	if fp.dest == nil {
		glog.V(1).Infof("Creating directory %q...", fp.path)
		atomic.AddUint64(&u.stats.CreatedDirectories, 1)
		// We force u+w so we can continue working on the directory.
		return u.dest.Mkdir(fp.path, fp.src.Mode()&commonModeMask|0200, attrs.UID, attrs.GID)
	}

	glog.V(1).Infof("Updating directory %q (%+v %+v)...", fp.path, fp.src.ModTime(), fp.dest.ModTime())
	// We force u+w so we can continue working on the directory.
	if err := u.dest.Chmod(fp.path, fp.src.Mode()&commonModeMask|0200); err != nil {
		return err
	}
	if err := u.dest.Lchown(fp.path, attrs.UID, attrs.GID); err != nil {
		return err
	}
	atomic.AddUint64(&u.stats.UpdatedDirectories, 1)

	return nil
}

// Stats returns statistics about the in-progress upload. This may be
// invoked while Run is executing.
func (u *Upload) Stats() UploadStats {
	us := UploadStats{
		InodeTable: uint32(u.srcLinks.Len()),

		UploadedBytes: atomic.LoadUint64(&u.stats.UploadedBytes),
		UploadedFiles: atomic.LoadUint64(&u.stats.UploadedFiles),

		CreatedDirectories: atomic.LoadUint64(&u.stats.CreatedDirectories),
		UpdatedDirectories: atomic.LoadUint64(&u.stats.UpdatedDirectories),

		KeptBytes:       atomic.LoadUint64(&u.stats.KeptBytes),
		KeptFiles:       atomic.LoadUint64(&u.stats.KeptFiles),
		KeptDirectories: atomic.LoadUint64(&u.stats.KeptDirectories),

		RemovedFiles:       atomic.LoadUint64(&u.stats.RemovedFiles),
		RemovedDirectories: atomic.LoadUint64(&u.stats.RemovedDirectories),

		DiscardedFiles: atomic.LoadUint64(&u.stats.DiscardedFiles),

		lastPath: u.stats.lastPath,
	}
	us.ProcessStats.CopyFrom(&u.stats.ProcessStats)
	return us
}

// UploadStats contains a snapshot of upload statistics.
type UploadStats struct {
	ProcessStats

	InodeTable uint32

	UploadedBytes uint64
	UploadedFiles uint64

	CreatedDirectories uint64
	UpdatedDirectories uint64

	KeptBytes       uint64
	KeptFiles       uint64
	KeptDirectories uint64

	RemovedFiles       uint64
	RemovedDirectories uint64

	DiscardedFiles uint64

	lastPath *atomic.Value // *filePath
}

// LastPath returns the last path the upload transfer touched.
func (us *UploadStats) LastPath() string {
	if fp, ok := us.lastPath.Load().(*filePair); ok {
		return string(fp.path)
	}
	return ""
}

// LastFileOperation returns the type of operation that was last
// performed.
func (us *UploadStats) LastFileOperation() FileOperation {
	if fp, ok := us.lastPath.Load().(*filePair); ok {
		switch {
		case fp.src != nil && fp.dest != nil:
			return Keep
		case fp.src != nil:
			return Create
		case fp.dest != nil:
			return Remove
		}
	}
	return UnknownFileOperation
}
