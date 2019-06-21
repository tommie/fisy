package fs

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/sync/errgroup"
)

// COW is a copy-on-write file system.
type COW struct {
	fs    WriteableFileSystem
	rroot Path
	wroot Path

	initOnce  sync.Once
	initGroup errgroup.Group
}

const (
	latestPath     Path = ".latest"
	completeSuffix Path = ".complete"
)

// NewCOW returns a new copy-on-write file system at the given
// location, for a given hostname and timestamp.
func NewCOW(fs WriteableFileSystem, host string, t time.Time) (*COW, error) {
	if host == "" {
		return nil, errors.New("host must be non-empty")
	}

	ts := Path(t.Format("2006-01-02T15-04-05.000000"))
	rdir, err := fs.Readlink(Path(host).Resolve(latestPath))
	if IsNotExist(err) {
		rdir, err = fs.Readlink(latestPath)
		if IsNotExist(err) {
			rdir = ts
		} else if err != nil {
			return nil, err
		}
	}
	if ts < rdir {
		return nil, fmt.Errorf("there is a newer timestamp already: new %v, existing %v", ts, rdir)
	}

	return &COW{
		fs:    fs,
		rroot: Path(host).Resolve(rdir),
		wroot: Path(host).Resolve(ts),
	}, nil
}

func (fs *COW) init() error {
	fs.initOnce.Do(func() {
		fs.initGroup.Go(func() error {
			if err := fs.fs.Mkdir(fs.wroot.Dir(), 0750, -1, -1); err != nil && !IsExist(err) {
				return err
			}
			if err := fs.fs.Mkdir(fs.wroot, 0750, -1, -1); err != nil {
				return err
			}

			return nil
		})
	})
	return fs.initGroup.Wait()
}

func (fs *COW) atomicSymlink(oldpath Path, newpath Path) error {
	tmp := newpath.Dir().Resolve(".new")
	if err := fs.fs.Symlink(oldpath, tmp); err != nil {
		return err
	}
	return fs.fs.Rename(tmp, newpath)
}

func (fs *COW) Open(path Path) (FileReader, error) {
	return fs.fs.Open(fs.rroot.Resolve(path))
}

func (fs *COW) Readlink(path Path) (Path, error) {
	return fs.fs.Readlink(fs.rroot.Resolve(path))
}

func (fs *COW) Stat() (FSInfo, error) {
	return fs.fs.Stat()
}

func (fs *COW) Finish() error {
	// Mark it as complete.
	if err := fs.atomicSymlink(fs.wroot.Base(), fs.wroot.Dir().Resolve(fs.wroot.Base()+completeSuffix)); err != nil {
		return err
	}
	// Mark it as the latest in this host.
	if err := fs.atomicSymlink(fs.wroot.Base(), fs.wroot.Dir().Resolve(latestPath)); err != nil {
		return err
	}
	// Mark it as the latest overall.
	return fs.atomicSymlink(fs.wroot, latestPath)
}

func (fs *COW) Create(path Path) (FileWriter, error) {
	if err := fs.init(); err != nil {
		return nil, err
	}
	return fs.fs.Create(fs.wroot.Resolve(path))
}

func (fs *COW) Keep(path Path) error {
	if err := fs.init(); err != nil {
		return err
	}

	err := fs.fs.Link(fs.rroot.Resolve(path), fs.wroot.Resolve(path))
	if err == nil || !IsPermission(err) {
		return err
	}

	// We failed to hardlink, so it's probably a directory.
	fr, err := fs.fs.Open(fs.rroot.Resolve(path))
	if err != nil {
		return err
	}
	defer fr.Close()

	fi, err := fr.Stat()
	if err != nil {
		return err
	}
	uid := -1
	gid := -1
	if fs, ok := fi.Sys().(*sftp.FileStat); ok {
		uid = int(fs.UID)
		gid = int(fs.GID)
	}

	// We force u+w so we can continue working on the directory.
	return fs.fs.Mkdir(fs.wroot.Resolve(path), fi.Mode()|0200, uid, gid)
}

func (fs *COW) Mkdir(path Path, mode os.FileMode, uid, gid int) error {
	if err := fs.init(); err != nil {
		return err
	}
	return fs.fs.Mkdir(fs.wroot.Resolve(path), mode, uid, gid)
}

func (fs *COW) Link(oldpath Path, newpath Path) error {
	if err := fs.init(); err != nil {
		return err
	}
	return fs.fs.Link(fs.wroot.Resolve(oldpath), fs.wroot.Resolve(newpath))
}

func (fs *COW) Symlink(oldpath Path, newpath Path) error {
	if err := fs.init(); err != nil {
		return err
	}
	return fs.fs.Symlink(oldpath, fs.wroot.Resolve(newpath))
}

func (fs *COW) Rename(oldpath Path, newpath Path) error {
	if err := fs.fs.Keep(oldpath); err != nil {
		return err
	}
	return fs.fs.Rename(fs.wroot.Resolve(oldpath), fs.wroot.Resolve(newpath))
}

func (fs *COW) RemoveAll(path Path) error {
	// Nothing to do.
	return nil
}

func (fs *COW) Remove(path Path) error {
	// Nothing to do.
	return nil
}

func (fs *COW) Chmod(path Path, mode os.FileMode) error {
	return fs.fs.Chmod(fs.wroot.Resolve(path), mode)
}

func (fs *COW) Lchown(path Path, uid, gid int) error {
	return fs.fs.Lchown(fs.wroot.Resolve(path), uid, gid)
}

func (fs *COW) Chtimes(path Path, atime time.Time, mtime time.Time) error {
	return fs.fs.Chtimes(fs.wroot.Resolve(path), atime, mtime)
}
