package fs

import (
	"io"
	"os"
	"path/filepath"
	"time"
)

type ReadableFileSystem interface {
	Open(path Path) (FileReader, error)
	Readlink(path Path) (Path, error)

	Stat() (FSInfo, error)
}

type WriteableFileSystem interface {
	ReadableFileSystem

	Create(path Path) (FileWriter, error)
	Mkdir(path Path, mode os.FileMode, uid, gid int) error
	Link(oldpath Path, newpath Path) error
	Symlink(oldpath Path, newpath Path) error
	Rename(oldpath Path, newpath Path) error
	RemoveAll(path Path) error
	Remove(path Path) error
	Keep(path Path) error

	Chmod(Path, os.FileMode) error
	Lchown(path Path, uid, gid int) error
	Chtimes(path Path, atime time.Time, mtime time.Time) error
}

type FileReader interface {
	io.Reader
	io.Closer

	Readdir() ([]os.FileInfo, error)
	Stat() (os.FileInfo, error)
}

type FileWriter interface {
	io.Writer
	io.Closer

	Chmod(os.FileMode) error
	Chown(uid, gid int) error
}

type Path string

func (p Path) Base() Path {
	return Path(filepath.Base(string(p)))
}

func (p Path) Dir() Path {
	return Path(filepath.Dir(string(p)))
}

func (p Path) Resolve(pp Path) Path {
	return Path(filepath.Join(string(p), string(pp)))
}

type FSInfo struct {
	FreeSpace uint64
}
