package fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pkg/sftp"
)

// A ReadableFileSystem can only be read from.
type ReadableFileSystem interface {
	// Open opens a file or directory for reading.
	Open(path Path) (FileReader, error)

	// Readlink returns the contents of the given symlink.
	Readlink(path Path) (Path, error)

	// Stat returns information about this file system.
	Stat() (FSInfo, error)
}

// A WriteableFileSystem can be both used to both read and write files.
type WriteableFileSystem interface {
	ReadableFileSystem

	// Create creates (or overwrites) a file and opens it for writing.
	Create(path Path) (FileWriter, error)

	// Keep informs the file system that the file should be kept.
	Keep(path Path) error

	// Mkdir creates a new directory. If uid or gid are -1, that value is ignored.
	Mkdir(path Path, mode os.FileMode, uid, gid int) error

	// Link creates a hardlink to an existing file.
	Link(oldpath Path, newpath Path) error

	// Symlink creates a symlink pointing to a file or directory.
	Symlink(oldpath Path, newpath Path) error

	// Rename moves a file or directory from one path to another.
	Rename(oldpath Path, newpath Path) error

	// RemoveAll recursively deletes a directory (or file).
	RemoveAll(path Path) error

	// Remove deletes a file or empty directory.
	Remove(path Path) error

	// Chmod changes file or directory modes and permissions.
	Chmod(Path, os.FileMode) error

	// Lchown changes the owner or group of a file or directory.
	// If uid or gid are -1, that value is ignored. Symlinks are updated, not followed.
	Lchown(path Path, uid, gid int) error

	// Chtimes modifies the file or directory metadata for access and modification times.
	Chtimes(path Path, atime time.Time, mtime time.Time) error
}

// A FileReader represents an open file stream or directory that can be read from.
type FileReader interface {
	io.Reader
	io.Closer

	// Readdir returns all directory entries, if the file represents a directory.
	Readdir() ([]os.FileInfo, error)

	// Stat returns metadata about the file.
	Stat() (os.FileInfo, error)
}

// A FileWriter represents an open file stream that can be written to.
type FileWriter interface {
	io.Writer
	io.Closer

	// Chmod changes file modes and permissions.
	Chmod(os.FileMode) error

	// Lchown changes the owner or group of the file.
	// If uid or gid are -1, that value is ignored.
	Chown(uid, gid int) error
}

// A Path points to a file or directory within a file system. They are
// always relative the root of the file system and never starts with a
// directory separator.
type Path string

// Base returns the last component of the path.
func (p Path) Base() Path {
	return Path(filepath.Base(string(p)))
}

// Dir returns all but the last component of the path.
func (p Path) Dir() Path {
	return Path(filepath.Dir(string(p)))
}

// Resolve looks up "pp" in the context of "p".
func (p Path) Resolve(pp Path) Path {
	return Path(filepath.Join(string(p), string(pp)))
}

// FSInfo carries statistics about a file system.
type FSInfo struct {
	// FreeSpace describes how many bytes are available for use.
	FreeSpace uint64
}

// uidGidFromFileInfo extracts user/group information from a FileInfo.
func uidGidFromFileInfo(fi os.FileInfo) (uid int, gid int, err error) {
	if fs, ok := fi.Sys().(*sftp.FileStat); ok {
		uid = int(fs.UID)
		gid = int(fs.GID)
		return
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		uid = int(st.Uid)
		gid = int(st.Gid)
		return
	}
	return -1, -1, fmt.Errorf("no UID/GID information for %q", fi.Name())
}
