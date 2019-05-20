package transfer

import (
	"context"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/tommie/fisy/fs"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const commonModeMask os.FileMode = 0xFFFFF

type Upload struct {
	src  fs.ReadableFileSystem
	dest fs.WriteableFileSystem

	srcInodes map[uint64]*inodeInfo
	c         *sync.Cond
	sem       *semaphore.Weighted

	stats     UploadStats
}

type inodeInfo struct {
	path     fs.Path
	uploaded bool
	nlink    int
}

type filePair struct {
	path        fs.Path
	src         os.FileInfo
	dest        os.FileInfo
	linkToInode uint64
}

func NewUpload(dest fs.WriteableFileSystem, src fs.ReadableFileSystem) *Upload {
	return &Upload{
		src:  src,
		dest: dest,

		srcInodes: map[uint64]*inodeInfo{},
		c:         sync.NewCond(&sync.Mutex{}),
		sem:       semaphore.NewWeighted(128),

		stats: UploadStats{
			lastPath: &atomic.Value{},
		},
	}
}

func (u *Upload) Run(ctx context.Context) (err error) {
	var eg errgroup.Group

	fps, err := u.listDir(fs.Path("."))
	if err != nil {
		return err
	}

	for _, fp := range fps {
		fp := fp
		eg.Go(func() error {
			return u.process(ctx, fp)
		})
	}

	return eg.Wait()
}

func (u *Upload) process(ctx context.Context, fp *filePair) error {
	var fps []*filePair
	err := func() error {
		if err := u.sem.Acquire(ctx, 1); err != nil {
			return err
		}
		defer u.sem.Release(1)

		if fp.src != nil {
			if fp.src.IsDir() {
				atomic.AddUint64(&u.stats.SourceDirectories, 1)
			} else {
				atomic.AddUint64(&u.stats.SourceBytes, uint64(fp.src.Size()))
				atomic.AddUint64(&u.stats.SourceFiles, 1)
			}
		}

		var eg errgroup.Group
		if fp.src != nil && fp.src.IsDir() {
			eg.Go(func() error {
				var err error
				fps, err = u.listDir(fp.path)
				return err
			})
		}
		eg.Go(func() error {
			return u.transfer(fp)
		})
		return eg.Wait()
	}()
	if err != nil {
		return err
	}

	var eg errgroup.Group
	for _, fp := range fps {
		fp := fp
		eg.Go(func() error {
			return u.process(ctx, fp)
		})
	}
	return eg.Wait()
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
		if fp.src != nil {
			if st, ok := fp.src.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
				u.c.L.Lock()
				if _, ok := u.srcInodes[st.Ino]; !ok {
					u.srcInodes[st.Ino] = &inodeInfo{
						path: fp.path,
						// Discount the first link.
						nlink: int(st.Nlink) - 1,
					}
				} else {
					fp.linkToInode = st.Ino
				}
				u.c.L.Unlock()
			}
		}
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
	u.stats.lastPath.Store(string(fp.path))

	if fp.src == nil {
		if fp.dest.IsDir() {
			// Removed directory.
			glog.V(1).Infof("Removing directory %q...", fp.path)
			atomic.AddUint64(&u.stats.RemovedDirectories, 1)
			return u.dest.RemoveAll(fp.path)
		}

		// Removed file.
		glog.V(1).Infof("Removing file %q...", fp.path)
		atomic.AddUint64(&u.stats.RemovedFiles, 1)
		return u.dest.Remove(fp.path)
	}

	if fp.dest != nil && !needsTransfer(fp.dest, fp.src) {
		glog.V(1).Infof("Keeping file %q...", fp.path)
		if err := u.dest.Keep(fp.path); err == nil {
			if fp.dest.IsDir() {
				atomic.AddUint64(&u.stats.KeptDirectories, 1)
			} else {
				atomic.AddUint64(&u.stats.KeptBytes, uint64(fp.dest.Size()))
				atomic.AddUint64(&u.stats.KeptFiles, 1)
			}
			return nil
		} else {
			glog.V(2).Infof("Failed to keep: %v", err)
		}

		// Fall back to normal transfer.
	}

	if fp.src.IsDir() {
		// Directory.
		uid := -1
		gid := -1
		if sstat, ok := fp.src.Sys().(*syscall.Stat_t); ok {
			uid = int(sstat.Uid)
			gid = int(sstat.Gid)
		}

		if fp.dest == nil {
			glog.V(1).Infof("Creating directory %q...", fp.path)
			atomic.AddUint64(&u.stats.CreatedDirectories, 1)
			return u.dest.Mkdir(fp.path, fp.src.Mode()&commonModeMask, uid, gid)
		}

		glog.V(1).Infof("Updating directory %q (%+v %+v)...", fp.path, fp.src.ModTime(), fp.dest.ModTime())
		if err := u.dest.Chmod(fp.path, fp.src.Mode()&commonModeMask); err != nil {
			return err
		}
		if err := u.dest.Lchown(fp.path, uid, gid); err != nil {
			return err
		}
		atomic.AddUint64(&u.stats.UpdatedDirectories, 1)
		return nil
	}

	if fp.linkToInode != 0 {
		u.c.L.Lock()
		if firstPath := u.srcInodes[fp.linkToInode].path; firstPath != fp.path {
			// We should hardlink. It is safe to block
			// here since we know firstPath was
			// transferred before us.
			for !u.srcInodes[fp.linkToInode].uploaded {
				u.c.Wait()
			}
			u.srcInodes[fp.linkToInode].nlink--
			if u.srcInodes[fp.linkToInode].nlink == 0 {
				// Clean up. We don't need this in memory anymore.
				delete(u.srcInodes, fp.linkToInode)
			}
			u.c.L.Unlock()
			glog.V(1).Infof("Hard-linking file %q to %q...", fp.path, firstPath)
			atomic.AddUint64(&u.stats.UploadedFiles, 1)
			return u.dest.Link(firstPath, fp.path)
		}
		u.c.L.Unlock()

		defer func() {
			u.c.L.Lock()
			defer u.c.L.Unlock()

			// If we failed to upload, this will cause
			// other transfers to fail as well.
			u.srcInodes[fp.linkToInode].uploaded = true
			u.srcInodes[fp.linkToInode].nlink--
			u.c.Broadcast()
		}()
	}

	sf, err := u.src.Open(fp.path)
	if err != nil {
		return err
	}
	defer sf.Close()

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

	atime := fp.src.ModTime()
	df, err := u.dest.Create(fp.path)
	if err != nil {
		return err
	}

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
		u.dest.Remove(fp.path)
		df.Close()
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

func needsTransfer(dest, src os.FileInfo) bool {
	if dest.IsDir() {
		return dest.Mode()&commonModeMask != src.Mode()&commonModeMask
	}
	return dest.Size() != src.Size() ||
		dest.Mode()&commonModeMask != src.Mode()&commonModeMask ||
		!dest.ModTime().Truncate(time.Second).Equal(src.ModTime().Truncate(time.Second))
}

func (u *Upload) Stats() UploadStats {
	return UploadStats{
		SourceBytes: atomic.LoadUint64(&u.stats.SourceBytes),
		SourceFiles: atomic.LoadUint64(&u.stats.SourceFiles),
		SourceDirectories: atomic.LoadUint64(&u.stats.SourceDirectories),

		UploadedBytes: atomic.LoadUint64(&u.stats.UploadedBytes),
		UploadedFiles: atomic.LoadUint64(&u.stats.UploadedFiles),

		CreatedDirectories: atomic.LoadUint64(&u.stats.CreatedDirectories),
		UpdatedDirectories: atomic.LoadUint64(&u.stats.UpdatedDirectories),

		KeptBytes: atomic.LoadUint64(&u.stats.KeptBytes),
		KeptFiles: atomic.LoadUint64(&u.stats.KeptFiles),
		KeptDirectories: atomic.LoadUint64(&u.stats.KeptDirectories),

		RemovedFiles: atomic.LoadUint64(&u.stats.RemovedFiles),
		RemovedDirectories: atomic.LoadUint64(&u.stats.RemovedDirectories),

		lastPath: u.stats.lastPath,
	}
}

type UploadStats struct {
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

	lastPath *atomic.Value // string
}

func (us *UploadStats) LastPath() string {
	if s, ok := us.lastPath.Load().(string); ok {
		return s
	}
	return ""
}
