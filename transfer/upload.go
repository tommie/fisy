package transfer

import (
	"context"
	"errors"
	"io"
	"os"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/tommie/fisy/fs"
	"github.com/tommie/fisy/remote"
)

// An Upload contains information about an in-progress upload. While
// Run is executing, Stats can be used to get progress information.
type Upload struct {
	process

	srcLinks linkSet
	gidMap   func(int) int
	uidMap   func(int) int

	stats    UploadStats
	fileHook FileHook
}

// NewUpload creates a new upload, with the given destination and source.
func NewUpload(dest fs.WriteableFileSystem, src fs.ReadableFileSystem, opts ...UploadOpt) *Upload {
	u := &Upload{
		process: process{
			src:  src,
			dest: dest,
		},

		srcLinks: newLinkSet(),
		gidMap:   func(srcGID int) int { return srcGID },
		uidMap:   func(srcUID int) int { return srcUID },

		stats: UploadStats{
			lastPair: &atomic.Value{},
		},
		fileHook: func(os.FileInfo, FileOperation, error) {},
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

// WithFileHook sets the per-file hook function. This is invoked when
// a file is starting transfer (with error set to InProgress), and
// when transfer has completed.
func WithFileHook(fun FileHook) UploadOpt {
	return func(u *Upload) {
		u.fileHook = fun
	}
}

// WithGIDMap sets a mapping from source GID to destination. By
// default, this is the identity function. The special value -1 can be
// both input and output, and means "current group".
func WithGIDMap(fun func(srcGID int) int) UploadOpt {
	return func(u *Upload) {
		u.gidMap = fun
	}
}

// WithUIDMap sets a mapping from source UID to destination. By
// default, this is the identity function. The special value -1 can be
// both input and output, and means "current user".
func WithUIDMap(fun func(srcUID int) int) UploadOpt {
	return func(u *Upload) {
		u.uidMap = fun
	}
}

// Transfer ensures a single file or directory has been fully
// transferred. It may do retries in case of failure.
func (u *Upload) transfer(ctx context.Context, fp *filePair) error {
	var nattempts int

	u.fileHook(fp.FileInfo(), fp.FileOperation(), InProgress)
	err := remote.Idempotent(ctx, func() error {
		u.stats.lastPair.Store(fp)

		nattempts++
		if nattempts > 1 {
			atomic.AddUint64(&u.stats.TransferRetries, 1)
		}

		switch fp.FileInfo().Mode().Type() {
		case os.ModeDir:
			return u.transferDirectory(fp)

		case 0, os.ModeSymlink:
			return u.transferFile(fp)

		default:
			glog.Infof("Ignored special file %q (type %s).", fp.path, fp.FileInfo().Mode().Type().String())
			return nil
		}
	})
	u.fileHook(fp.FileInfo(), fp.FileOperation(), err)
	return err
}

var errDiscarded = errors.New("file discarded")

// transferFile transfers a single file from source to dest.
func (u *Upload) transferFile(fp *filePair) (rerr error) {
	if fp.src == nil {
		// Removed file.
		glog.V(1).Infof("Removing file %q...", fp.path)
		if err := u.dest.Remove(fp.path); err != nil {
			return err
		}
		atomic.AddUint64(&u.stats.RemovedFiles, 1)
		return nil
	}

	inode, firstPath := u.srcLinks.FinishedFile(fp.path, fp.src)
	if inode != 0 {
		if firstPath != "" {
			glog.V(1).Infof("Hard-linking file %q to %q...", fp.path, firstPath)
			atomic.AddUint64(&u.stats.UploadedFiles, 1)
			return u.dest.Link(firstPath, fp.path)
		}

		defer func() {
			if rerr != nil {
				u.srcLinks.Discard(inode, fp.path)
			} else {
				u.srcLinks.Fulfill(inode)
			}
			if rerr == errDiscarded {
				rerr = nil
			}
		}()
	}

	if !fileNeedsTransfer(fp.dest, fp.src) {
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
		return u.createSymlink(fp)
	}

	return u.copyFile(fp)
}

func (u *Upload) createSymlink(fp *filePair) error {
	linkdest, err := u.src.Readlink(fp.path)
	if err != nil {
		if fs.IsNotExist(err) {
			// The symlink was removed between listing and transferring.
			atomic.AddUint64(&u.stats.DiscardedFiles, 1)
			return errDiscarded
		}
		return err
	}
	glog.V(1).Infof("Symlinking %q to %q...", fp.path, linkdest)
	atomic.AddUint64(&u.stats.UploadedBytes, uint64(len(linkdest)))
	atomic.AddUint64(&u.stats.UploadedFiles, 1)
	// TODO: The SFTP client library doesn't support the
	// lsetstat@openssh.com extension, so mtime will never be
	// right. Until there is support, we have no choice but to
	// always re-upload symlinks.
	return u.dest.Symlink(linkdest, fp.path)
}

// copyFile copies a file byte-by-byte.
func (u *Upload) copyFile(fp *filePair) error {
	sf, err := u.src.Open(fp.path)
	if err != nil {
		if fs.IsNotExist(err) {
			// The file was removed between listing and transferring.
			atomic.AddUint64(&u.stats.DiscardedFiles, 1)
			return errDiscarded
		}
		return err
	}
	defer sf.Close()

	df, err := u.dest.Create(fp.path)
	if fs.IsPermission(err) {
		// Remove the destination file and try again.
		u.dest.Remove(fp.path)
		df, err = u.dest.Create(fp.path)
	}
	if err != nil {
		return err
	}

	var uploadedBytes uint64
	err = func() error {
		atime := fp.src.ModTime()
		err := func() error {
			if err := df.Chmod(fp.src.Mode() & commonModeMask); err != nil {
				return err
			}

			glog.V(1).Infof("Uploading file %q (%d bytes)...", fp.path, fp.src.Size())
			i, err := io.Copy(df, sf)
			if err != nil {
				return err
			}
			uploadedBytes = uint64(i)

			attrs, ok := fs.FileAttrsFromFileInfo(fp.src)
			if ok {
				atime = attrs.AccessTime
			} else {
				attrs.UID = -1
				attrs.GID = -1
			}
			uid := u.uidMap(attrs.UID)
			gid := u.gidMap(attrs.GID)
			if uid != -1 || gid != -1 {
				if err := df.Chown(uid, gid); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			df.Close()
			return err
		}

		if err := df.Close(); err != nil {
			return err
		}

		return u.dest.Chtimes(fp.path, atime, fp.src.ModTime())
	}()
	if err != nil {
		u.dest.Remove(fp.path)
		return err
	}

	atomic.AddUint64(&u.stats.UploadedBytes, uploadedBytes)
	atomic.AddUint64(&u.stats.UploadedFiles, 1)

	return nil
}

// transferDirectory transfers a single directory.
func (u *Upload) transferDirectory(fp *filePair) error {
	if fp.src == nil {
		// Removed directory.
		glog.V(1).Infof("Removing directory %q...", fp.path)
		if err := u.dest.RemoveAll(fp.path); err != nil {
			return err
		}
		atomic.AddUint64(&u.stats.RemovedDirectories, 1)
		return nil
	}

	if !directoryNeedsTransfer(fp.dest, fp.src) {
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
		attrs.AccessTime = fp.src.ModTime()
	}

	if fp.dest == nil {
		glog.V(1).Infof("Creating directory %q...", fp.path)

		// We force u+w so we can continue working on the directory.
		if err := u.dest.Mkdir(fp.path, fp.src.Mode()&commonModeMask|0200, u.uidMap(attrs.UID), u.gidMap(attrs.GID)); err != nil {
			return err
		}
	} else {
		glog.V(1).Infof("Updating directory %q (%+v %+v)...", fp.path, fp.src.ModTime(), fp.dest.ModTime())

		// We force u+w so we can continue working on the directory.
		if err := u.dest.Mkdir(fp.path, fp.src.Mode()&commonModeMask|0200, u.uidMap(attrs.UID), u.gidMap(attrs.GID)); fs.IsExist(err) {
			// ErrExist is ignored to emulate "overwrite" just like Create does for files.

			uid := u.uidMap(attrs.UID)
			gid := u.gidMap(attrs.GID)
			if uid != -1 || gid != -1 {
				if err := u.dest.Lchown(fp.path, u.uidMap(attrs.UID), u.gidMap(attrs.GID)); err != nil {
					return err
				}
			}

			// We force u+w so we can continue working on the directory.
			if err := u.dest.Chmod(fp.path, fp.src.Mode()&commonModeMask|0200); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}

	// This might be overwritten if we transfer files into the directory.
	if err := u.dest.Chtimes(fp.path, attrs.AccessTime, fp.src.ModTime()); err != nil {
		return err
	}

	if fp.dest == nil {
		atomic.AddUint64(&u.stats.CreatedDirectories, 1)
	} else {
		atomic.AddUint64(&u.stats.UpdatedDirectories, 1)
	}

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

		DiscardedFiles:  atomic.LoadUint64(&u.stats.DiscardedFiles),
		TransferRetries: atomic.LoadUint64(&u.stats.TransferRetries),

		lastPair: u.stats.lastPair,
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

	DiscardedFiles  uint64
	TransferRetries uint64

	lastPair *atomic.Value // *filePair
}

// LastPath returns the last path the upload transfer touched.
func (us *UploadStats) LastPath() string {
	if fp, ok := us.lastPair.Load().(*filePair); ok {
		return string(fp.path)
	}
	return ""
}

// LastFileOperation returns the type of operation that was last
// performed.
func (us *UploadStats) LastFileOperation() FileOperation {
	if fp, ok := us.lastPair.Load().(*filePair); ok {
		return fp.FileOperation()
	}
	return UnknownFileOperation
}

// SetLast sets the last operation. This is only used in testing.
func (us *UploadStats) SetLast(path fs.Path, dest, src os.FileInfo) {
	if us.lastPair == nil {
		us.lastPair = &atomic.Value{}
	}
	us.lastPair.Store(&filePair{path: path, dest: dest, src: src})
}
