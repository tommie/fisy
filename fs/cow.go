package fs

import (
	"errors"
	"os"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/sync/errgroup"
)

type baseCOW struct {
	fs    WriteableFileSystem
	rroot Path
	wroot Path
}

func (fs *baseCOW) Open(path Path) (FileReader, error) {
	return fs.fs.Open(fs.rroot.Resolve(path))
}

func (fs *baseCOW) Readlink(path Path) (Path, error) {
	return fs.fs.Readlink(fs.rroot.Resolve(path))
}

func (fs *baseCOW) Create(path Path) (FileWriter, error) {
	return fs.fs.Create(fs.wroot.Resolve(path))
}

func (fs *baseCOW) Keep(path Path) error {
	err := fs.fs.Link(fs.rroot.Resolve(path), fs.wroot.Resolve(path))
	if err == nil || !IsPermission(err) {
		return err
	}

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

func (fs *baseCOW) Mkdir(path Path, mode os.FileMode, uid, gid int) error {
	return fs.fs.Mkdir(fs.wroot.Resolve(path), mode, uid, gid)
}

func (fs *baseCOW) Link(oldpath Path, newpath Path) error {
	return fs.fs.Link(fs.wroot.Resolve(oldpath), fs.wroot.Resolve(newpath))
}

func (fs *baseCOW) Symlink(oldpath Path, newpath Path) error {
	return fs.fs.Symlink(oldpath, fs.wroot.Resolve(newpath))
}

func (fs *baseCOW) Rename(oldpath Path, newpath Path) error {
	if err := fs.fs.Keep(oldpath); err != nil {
		return err
	}
	return fs.fs.Rename(fs.wroot.Resolve(oldpath), fs.wroot.Resolve(newpath))
}

func (fs *baseCOW) RemoveAll(path Path) error {
	// Nothing to do.
	return nil
}

func (fs *baseCOW) Remove(path Path) error {
	// Nothing to do.
	return nil
}

func (fs *baseCOW) Stat() (FSInfo, error) {
	return fs.fs.Stat()
}

func (fs *baseCOW) Chmod(path Path, mode os.FileMode) error {
	return fs.fs.Chmod(fs.wroot.Resolve(path), mode)
}

func (fs *baseCOW) Lchown(path Path, uid, gid int) error {
	return fs.fs.Lchown(fs.wroot.Resolve(path), uid, gid)
}

func (fs *baseCOW) Chtimes(path Path, atime time.Time, mtime time.Time) error {
	return fs.fs.Chtimes(fs.wroot.Resolve(path), atime, mtime)
}

type COW struct {
	baseCOW

	copyOnce  sync.Once
	copyGroup errgroup.Group
}

const (
	latestPath     Path = ".latest"
	completeSuffix Path = ".complete"
)

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

	return &COW{
		baseCOW: baseCOW{
			fs:    fs,
			rroot: Path(host).Resolve(rdir),
			wroot: Path(host).Resolve(ts),
		},
	}, nil
}

func (fs *COW) initCopy() error {
	fs.copyOnce.Do(func() {
		fs.copyGroup.Go(func() error {
			if err := fs.fs.Mkdir(fs.wroot.Dir(), 0750, -1, -1); err != nil && !IsExist(err) {
				return err
			}
			if err := fs.fs.Mkdir(fs.wroot, 0750, -1, -1); err != nil && !IsExist(err) {
				return err
			}

			return nil
		})
	})
	return fs.copyGroup.Wait()
}

func (fs *COW) atomicSymlink(oldpath Path, newpath Path) error {
	tmp := newpath.Dir().Resolve(".new")
	if err := fs.fs.Symlink(oldpath, tmp); err != nil {
		return err
	}
	return fs.fs.Rename(tmp, newpath)
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
	if err := fs.initCopy(); err != nil {
		return nil, err
	}
	return fs.baseCOW.Create(path)
}

func (fs *COW) Keep(path Path) error {
	if err := fs.initCopy(); err != nil {
		return err
	}
	return fs.baseCOW.Keep(path)
}

func (fs *COW) Mkdir(path Path, mode os.FileMode, uid, gid int) error {
	if err := fs.initCopy(); err != nil {
		return err
	}
	return fs.baseCOW.Mkdir(path, mode, uid, gid)
}

func (fs *COW) Link(oldpath Path, newpath Path) error {
	if err := fs.initCopy(); err != nil {
		return err
	}
	return fs.baseCOW.Link(oldpath, newpath)
}

func (fs *COW) Symlink(oldpath Path, newpath Path) error {
	if err := fs.initCopy(); err != nil {
		return err
	}
	return fs.baseCOW.Symlink(oldpath, newpath)
}
