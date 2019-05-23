package transfer

import (
	"context"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/tommie/fisy/fs"
	"golang.org/x/sync/errgroup"
)

type FileOperation rune

const (
	UnknownFileOperation FileOperation = '?'
	Create               FileOperation = 'C'
	Remove               FileOperation = 'R'
	Keep                 FileOperation = 'K'
)

const commonModeMask os.FileMode = 0xFFFFF

type Upload struct {
	src          fs.ReadableFileSystem
	dest         fs.WriteableFileSystem
	ignoreFilter func(fs.Path) bool

	srcLinks linkSet

	stats UploadStats
}

type filePair struct {
	path        fs.Path
	src         os.FileInfo
	dest        os.FileInfo
	linkToInode uint64
}

func NewUpload(dest fs.WriteableFileSystem, src fs.ReadableFileSystem, opts ...UploadOpt) *Upload {
	const conc = 128

	u := &Upload{
		src:          src,
		dest:         dest,
		ignoreFilter: func(fs.Path) bool { return false },

		srcLinks: newLinkSet(),

		stats: UploadStats{
			lastPath: &atomic.Value{},
		},
	}
	for _, opt := range opts {
		opt(u)
	}
	return u
}

type UploadOpt func(*Upload)

func WithIgnoreFilter(fun func(fs.Path) bool) UploadOpt {
	return func(u *Upload) {
		u.ignoreFilter = fun
	}
}

func (u *Upload) Run(ctx context.Context) error {
	fps, err := u.listDir(fs.Path("."))
	if err != nil {
		return err
	}

	return filePairPDFS(ctx, fps, u.process, 128)
}

func (u *Upload) process(ctx context.Context, fp *filePair) ([]*filePair, error) {
	atomic.AddUint32(&u.stats.InProgress, 1)
	defer atomic.AddUint32(&u.stats.InProgress, ^uint32(0))

	if fp.src != nil {
		if fp.src.IsDir() {
			atomic.AddUint64(&u.stats.SourceDirectories, 1)
		} else {
			atomic.AddUint64(&u.stats.SourceBytes, uint64(fp.src.Size()))
			atomic.AddUint64(&u.stats.SourceFiles, 1)
		}
	}

	isDir := fp.src != nil && fp.src.IsDir() || fp.dest != nil && fp.dest.IsDir()
	filterPath := "/" + fp.path
	if isDir {
		filterPath += "/"
	}
	if u.ignoreFilter(filterPath) {
		if isDir {
			atomic.AddUint64(&u.stats.IgnoredDirectories, 1)
		} else {
			atomic.AddUint64(&u.stats.IgnoredFiles, 1)
		}
		glog.V(3).Infof("Ignored %q.", fp.path)
		return nil, nil
	}

	var fps []*filePair
	var eg errgroup.Group
	if fp.src != nil && fp.src.IsDir() {
		eg.Go(func() error {
			var err error
			fps, err = u.listDir(fp.path)
			return err
		})
	}
	eg.Go(func() error {
		err := u.transfer(fp)
		if err != nil {
			glog.Errorf("Failed to transfer %q: %v", fp.path, err)
			glog.V(1).Infof("Source: %+v\nDestination: %+v", fp.src, fp.dest)
		}
		return err
	})
	return fps, eg.Wait()
}

func (u *Upload) listDir(path fs.Path) ([]*filePair, error) {
	var eg errgroup.Group
	var srcfiles, destfiles []os.FileInfo
	eg.Go(func() error {
		var err error
		srcfiles, err = readdir(u.src, path)
		if err != nil {
			return err
		}
		sort.Slice(srcfiles, func(i, j int) bool { return srcfiles[i].Name() < srcfiles[j].Name() })
		return nil
	})
	eg.Go(func() error {
		var err error
		destfiles, err = readdir(u.dest, path)
		if err != nil && !fs.IsNotExist(err) {
			return err
		}
		sort.Slice(destfiles, func(i, j int) bool { return destfiles[i].Name() < destfiles[j].Name() })
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Join the two sorted lists.
	var fps []*filePair
	var i, j int
	for i < len(srcfiles) && j < len(destfiles) {
		sf := srcfiles[i]
		df := destfiles[j]
		var name string
		if sf.Name() < df.Name() {
			// New file.
			df = nil
			name = sf.Name()
			i++
		} else if sf.Name() > df.Name() {
			// Removed file.
			sf = nil
			name = df.Name()
			j++
		} else {
			// In both.
			name = sf.Name()
			i++
			j++
		}
		fps = append(fps, &filePair{path: path.Resolve(fs.Path(name)), src: sf, dest: df})
	}
	for ; i < len(srcfiles); i++ {
		f := srcfiles[i]
		fps = append(fps, &filePair{path: path.Resolve(fs.Path(f.Name())), src: f})
	}
	for ; j < len(destfiles); j++ {
		f := destfiles[j]
		fps = append(fps, &filePair{path: path.Resolve(fs.Path(f.Name())), dest: f})
	}

	for _, fp := range fps {
		// Allow hardlinks if possible.
		u.srcLinks.Offer(fp)
	}

	return fps, nil
}

func readdir(fs fs.ReadableFileSystem, path fs.Path) ([]os.FileInfo, error) {
	fr, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	return fr.Readdir()
}

func (u *Upload) transfer(fp *filePair) (err error) {
	u.stats.lastPath.Store(fp)

	fi := fp.src
	if fi == nil {
		fi = fp.dest
	}

	if fi.IsDir() {
		return u.transferDirectory(fp)
	}

	return u.transferFile(fp)
}

func (u *Upload) transferFile(fp *filePair) (err error) {
	if fp.src == nil {
		// Removed file.
		glog.V(1).Infof("Removing file %q...", fp.path)
		atomic.AddUint64(&u.stats.RemovedFiles, 1)
		return u.dest.Remove(fp.path)
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

	if fp.linkToInode != 0 {
		if firstPath := u.srcLinks.FinishedLinkPath(fp); firstPath != "" {
			glog.V(1).Infof("Hard-linking file %q to %q...", fp.path, firstPath)
			atomic.AddUint64(&u.stats.UploadedFiles, 1)
			return u.dest.Link(firstPath, fp.path)
		}

		defer u.srcLinks.Fulfill(fp)
	}

	if fp.src.Mode()&os.ModeSymlink != 0 {
		// We should symlink.
		linkdest, err := u.src.Readlink(fp.path)
		if err != nil {
			return err
		}
		glog.V(1).Infof("Symlinking %q to %q...", fp.path, linkdest)
		atomic.AddUint64(&u.stats.UploadedBytes, uint64(len(linkdest)))
		atomic.AddUint64(&u.stats.UploadedFiles, 1)
		return u.dest.Symlink(linkdest, fp.path)
	}

	sf, err := u.src.Open(fp.path)
	if err != nil {
		return err
	}
	defer sf.Close()

	df, err := u.dest.Create(fp.path)
	if err != nil {
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

		if sstat, ok := fp.src.Sys().(*syscall.Stat_t); ok {
			if err := df.Chown(int(sstat.Uid), int(sstat.Gid)); err != nil {
				return err
			}
			atime = time.Unix(sstat.Atim.Sec, sstat.Atim.Nsec)
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

func (u *Upload) transferDirectory(fp *filePair) (err error) {
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

	uid := -1
	gid := -1
	if sstat, ok := fp.src.Sys().(*syscall.Stat_t); ok {
		uid = int(sstat.Uid)
		gid = int(sstat.Gid)
	}

	if fp.dest == nil {
		glog.V(1).Infof("Creating directory %q...", fp.path)
		atomic.AddUint64(&u.stats.CreatedDirectories, 1)
		// We force u+w so we can continue working on the directory.
		return u.dest.Mkdir(fp.path, fp.src.Mode()&commonModeMask|0200, uid, gid)
	}

	glog.V(1).Infof("Updating directory %q (%+v %+v)...", fp.path, fp.src.ModTime(), fp.dest.ModTime())
	// We force u+w so we can continue working on the directory.
	if err := u.dest.Chmod(fp.path, fp.src.Mode()&commonModeMask|0200); err != nil {
		return err
	}
	if err := u.dest.Lchown(fp.path, uid, gid); err != nil {
		return err
	}
	atomic.AddUint64(&u.stats.UpdatedDirectories, 1)

	return nil
}

func needsTransfer(dest, src os.FileInfo) bool {
	if dest.IsDir() {
		// We force u+w so we can continue working on the directory.
		return dest.Mode()&commonModeMask&^0200 != src.Mode()&commonModeMask&^0200
	}
	return dest.Size() != src.Size() ||
		dest.Mode()&commonModeMask != src.Mode()&commonModeMask ||
		!dest.ModTime().Truncate(time.Second).Equal(src.ModTime().Truncate(time.Second))
}

func (u *Upload) Stats() UploadStats {
	return UploadStats{
		InProgress: atomic.LoadUint32(&u.stats.InProgress),
		InodeTable: uint32(u.srcLinks.Len()),

		SourceBytes:       atomic.LoadUint64(&u.stats.SourceBytes),
		SourceFiles:       atomic.LoadUint64(&u.stats.SourceFiles),
		SourceDirectories: atomic.LoadUint64(&u.stats.SourceDirectories),

		UploadedBytes: atomic.LoadUint64(&u.stats.UploadedBytes),
		UploadedFiles: atomic.LoadUint64(&u.stats.UploadedFiles),

		CreatedDirectories: atomic.LoadUint64(&u.stats.CreatedDirectories),
		UpdatedDirectories: atomic.LoadUint64(&u.stats.UpdatedDirectories),

		KeptBytes:       atomic.LoadUint64(&u.stats.KeptBytes),
		KeptFiles:       atomic.LoadUint64(&u.stats.KeptFiles),
		KeptDirectories: atomic.LoadUint64(&u.stats.KeptDirectories),

		RemovedFiles:       atomic.LoadUint64(&u.stats.RemovedFiles),
		RemovedDirectories: atomic.LoadUint64(&u.stats.RemovedDirectories),

		IgnoredFiles:       atomic.LoadUint64(&u.stats.IgnoredFiles),
		IgnoredDirectories: atomic.LoadUint64(&u.stats.IgnoredDirectories),

		lastPath: u.stats.lastPath,
	}
}

type UploadStats struct {
	InProgress uint32
	InodeTable uint32

	SourceBytes       uint64
	SourceFiles       uint64
	SourceDirectories uint64

	UploadedBytes uint64
	UploadedFiles uint64

	CreatedDirectories uint64
	UpdatedDirectories uint64

	KeptBytes       uint64
	KeptFiles       uint64
	KeptDirectories uint64

	RemovedFiles       uint64
	RemovedDirectories uint64

	IgnoredFiles       uint64
	IgnoredDirectories uint64

	lastPath *atomic.Value // *filePath
}

func (us *UploadStats) LastPath() string {
	if fp, ok := us.lastPath.Load().(*filePair); ok {
		return string(fp.path)
	}
	return ""
}

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
