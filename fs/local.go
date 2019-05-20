package fs

import (
	"os"
	"time"
	"syscall"
)

type LocalFileSystem struct {
	root Path
}

func NewLocalFileSystem(root string) *LocalFileSystem {
	return &LocalFileSystem{root: Path(root)}
}

func (fs *LocalFileSystem) Open(path Path) (FileReader, error) {
	f, err := os.Open(string(fs.root.Resolve(path)))
	if err != nil {
		return nil, err
	}
	return &localFileReader{f}, nil
}

type localFileReader struct {
	*os.File
}

func (fr *localFileReader) Readdir() ([]os.FileInfo, error) {
	return fr.File.Readdir(0)
}

func (fs *LocalFileSystem) Readlink(path Path) (Path, error) {
	p, err := os.Readlink(string(fs.root.Resolve(path)))
	return Path(p), err
}

func (fs *LocalFileSystem) Create(path Path) (FileWriter, error) {
	p := string(fs.root.Resolve(path))
	f, err := os.Create(p)
	if IsPermission(err) {
		os.Remove(p)
		f, err = os.Create(p)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return &localFileWriter{f}, nil
}

type localFileWriter struct {
	*os.File
}

func (fs *LocalFileSystem) Keep(path Path) error {
	return nil
}

func (fs *LocalFileSystem) Mkdir(path Path, mode os.FileMode, uid, gid int) error {
	p := string(fs.root.Resolve(path))
	if err := os.Mkdir(p, mode); err != nil {
		return err
	}
	if err := os.Lchown(p, uid, gid); err != nil {
		os.Remove(p)
		return err
	}
	return nil
}

func (fs *LocalFileSystem) Link(oldpath Path, newpath Path) error {
	return os.Link(string(fs.root.Resolve(oldpath)), string(fs.root.Resolve(newpath)))
}

func (fs *LocalFileSystem) Symlink(oldpath Path, newpath Path) error {
	return os.Symlink(string(oldpath), string(fs.root.Resolve(newpath)))
}

func (fs *LocalFileSystem) Rename(oldpath Path, newpath Path) error {
	return os.Rename(string(fs.root.Resolve(oldpath)), string(fs.root.Resolve(newpath)))
}

func (fs *LocalFileSystem) RemoveAll(path Path) error {
	return os.RemoveAll(string(fs.root.Resolve(path)))
}

func (fs *LocalFileSystem) Remove(path Path) error {
	return os.Remove(string(fs.root.Resolve(path)))
}

func (fs *LocalFileSystem) Stat() (FSInfo, error) {
	var sf syscall.Statfs_t
	if err := syscall.Statfs(string(fs.root), &sf); err != nil {
		return FSInfo{}, err
	}
	return FSInfo{FreeSpace: uint64(sf.Frsize) * sf.Bavail}, nil
}

func (fs *LocalFileSystem) Chmod(path Path, mode os.FileMode) error {
	return os.Chmod(string(fs.root.Resolve(path)), mode)
}

func (fs *LocalFileSystem) Lchown(path Path, uid, gid int) error {
	return os.Lchown(string(fs.root.Resolve(path)), uid, gid)
}

func (fs *LocalFileSystem) Chtimes(path Path, atime time.Time, mtime time.Time) error {
	return os.Chtimes(string(fs.root.Resolve(path)), atime, mtime)
}
