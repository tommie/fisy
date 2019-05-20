package fs

import (
	"os"
	"sync"
	"time"
	"errors"

	"github.com/pkg/sftp"
	"golang.org/x/sync/errgroup"
)

type cowFileSystem struct {
	fs    WriteableFileSystem
	rroot Path
	wroot Path
}

func (fs *cowFileSystem) Open(path Path) (FileReader, error) {
	return fs.fs.Open(fs.rroot.Resolve(path))
}

func (fs *cowFileSystem) Readlink(path Path) (Path, error) {
	return fs.fs.Readlink(fs.rroot.Resolve(path))
}

func (fs *cowFileSystem) Create(path Path) (FileWriter, error) {
	return fs.fs.Create(fs.wroot.Resolve(path))
}

func (fs *cowFileSystem) Keep(path Path) error {
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

	return fs.fs.Mkdir(fs.wroot.Resolve(path), fi.Mode(), uid, gid)
}

func (fs *cowFileSystem) Mkdir(path Path, mode os.FileMode, uid, gid int) error {
	return fs.fs.Mkdir(fs.wroot.Resolve(path), mode, uid, gid)
}

func (fs *cowFileSystem) Link(oldpath Path, newpath Path) error {
	return fs.fs.Link(fs.rroot.Resolve(oldpath), fs.wroot.Resolve(newpath))
}

func (fs *cowFileSystem) Symlink(oldpath Path, newpath Path) error {
	return fs.fs.Symlink(oldpath, fs.wroot.Resolve(newpath))
}

func (fs *cowFileSystem) Rename(oldpath Path, newpath Path) error {
	if err := fs.fs.Keep(oldpath); err != nil {
		return err
	}
	return fs.fs.Rename(fs.wroot.Resolve(oldpath), fs.wroot.Resolve(newpath))
}

func (fs *cowFileSystem) RemoveAll(path Path) error {
	return fs.fs.RemoveAll(fs.wroot.Resolve(path))
}

func (fs *cowFileSystem) Remove(path Path) error {
	return fs.fs.Remove(fs.wroot.Resolve(path))
}

func (fs *cowFileSystem) Stat() (FSInfo, error) {
	return fs.fs.Stat()
}

func (fs *cowFileSystem) Chmod(path Path, mode os.FileMode) error {
	return fs.fs.Chmod(fs.wroot.Resolve(path), mode)
}

func (fs *cowFileSystem) Lchown(path Path, uid, gid int) error {
	return fs.fs.Lchown(fs.wroot.Resolve(path), uid, gid)
}

func (fs *cowFileSystem) Chtimes(path Path, atime time.Time, mtime time.Time) error {
	return fs.fs.Chtimes(fs.wroot.Resolve(path), atime, mtime)
}

type COWFileSystem struct {
	cowFileSystem

	copyOnce  sync.Once
	copyGroup errgroup.Group
}

const latestPath Path = ".latest"

func NewCOWFileSystem(fs WriteableFileSystem, host string, t time.Time) (*COWFileSystem, error) {
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

	return &COWFileSystem{
		cowFileSystem: cowFileSystem{
			fs:    fs,
			rroot: Path(host).Resolve(rdir),
			wroot: Path(host).Resolve(ts),
		},
	}, nil
}

func (fs *COWFileSystem) initCopy() error {
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

func (fs *COWFileSystem) atomicSymlink(oldpath Path, newpath Path) error {
	tmp := newpath.Dir().Resolve(".new")
	if err := fs.fs.Symlink(oldpath, tmp); err != nil {
		return err
	}
	return fs.fs.Rename(tmp, newpath)
}

func (fs *COWFileSystem) Finish() error {
	if err := fs.atomicSymlink(fs.wroot.Base(), fs.wroot.Dir().Resolve(latestPath)); err != nil {
		return err
	}
	return fs.atomicSymlink(fs.wroot, latestPath)
}

func (fs *COWFileSystem) Create(path Path) (FileWriter, error) {
	if err := fs.initCopy(); err != nil {
		return nil, err
	}
	return fs.cowFileSystem.Create(path)
}

func (fs *COWFileSystem) Keep(path Path) error {
	if err := fs.initCopy(); err != nil {
		return err
	}
	return fs.cowFileSystem.Keep(path)
}

func (fs *COWFileSystem) Mkdir(path Path, mode os.FileMode, uid, gid int) error {
	if err := fs.initCopy(); err != nil {
		return err
	}
	return fs.cowFileSystem.Mkdir(path, mode, uid, gid)
}

func (fs *COWFileSystem) Link(oldpath Path, newpath Path) error {
	if err := fs.initCopy(); err != nil {
		return err
	}
	return fs.cowFileSystem.Link(oldpath, newpath)
}

func (fs *COWFileSystem) Symlink(oldpath Path, newpath Path) error {
	if err := fs.initCopy(); err != nil {
		return err
	}
	return fs.cowFileSystem.Symlink(oldpath, newpath)
}
