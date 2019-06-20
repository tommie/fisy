package fs

import (
	"os"
	"syscall"
	"time"
)

// Test mock injection points.
var (
	syscallStatfs = syscall.Statfs
	osLchown      = os.Lchown
)

// Local is a file system working on the OS native file system.
type Local struct {
	root Path
}

// NewLocal constructs a new object to access the OS native file
// system.
func NewLocal(root string) *Local {
	return &Local{root: Path(root)}
}

// Open opens a file or directory for reading.
func (fs *Local) Open(path Path) (FileReader, error) {
	f, err := os.Open(string(fs.root.Resolve(path)))
	if err != nil {
		return nil, err
	}
	return &localFileReader{f}, nil
}

type localFileReader struct {
	*os.File
}

// Readdir returns all directory entries, if the file represents a directory.
func (fr *localFileReader) Readdir() ([]os.FileInfo, error) {
	return fr.File.Readdir(0)
}

// Readlink returns the contents of the given symlink.
func (fs *Local) Readlink(path Path) (Path, error) {
	p, err := os.Readlink(string(fs.root.Resolve(path)))
	return Path(p), err
}

// Stat returns information about this file system.
func (fs *Local) Stat() (FSInfo, error) {
	var sf syscall.Statfs_t
	if err := syscallStatfs(string(fs.root), &sf); err != nil {
		return FSInfo{}, err
	}
	return FSInfo{FreeSpace: uint64(sf.Frsize) * sf.Bavail}, nil
}

// Create creates (or overwrites) a file and opens it for writing.
func (fs *Local) Create(path Path) (FileWriter, error) {
	p := string(fs.root.Resolve(path))
	f, err := os.Create(p)
	if err != nil {
		return nil, err
	}
	return &localFileWriter{f}, nil
}

type localFileWriter struct {
	*os.File
}

// Keep informs the file system that the file should be kept. This does nothing.
func (fs *Local) Keep(path Path) error {
	return nil
}

// Mkdir creates a new directory. If uid or gid are -1, that value is ignored.
func (fs *Local) Mkdir(path Path, mode os.FileMode, uid, gid int) error {
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

// Link creates a hardlink to an existing file.
func (fs *Local) Link(oldpath Path, newpath Path) error {
	return os.Link(string(fs.root.Resolve(oldpath)), string(fs.root.Resolve(newpath)))
}

// Symlink creates a symlink pointing to a file or directory.
func (fs *Local) Symlink(oldpath Path, newpath Path) error {
	return os.Symlink(string(oldpath), string(fs.root.Resolve(newpath)))
}

// Rename moves a file or directory from one path to another.
func (fs *Local) Rename(oldpath Path, newpath Path) error {
	return os.Rename(string(fs.root.Resolve(oldpath)), string(fs.root.Resolve(newpath)))
}

// RemoveAll recursively deletes a directory (or file).
func (fs *Local) RemoveAll(path Path) error {
	return os.RemoveAll(string(fs.root.Resolve(path)))
}

// Remove deletes a file or empty directory.
func (fs *Local) Remove(path Path) error {
	return os.Remove(string(fs.root.Resolve(path)))
}

// Chmod changes file or directory modes and permissions.
func (fs *Local) Chmod(path Path, mode os.FileMode) error {
	return os.Chmod(string(fs.root.Resolve(path)), mode)
}

// Lchown changes the owner or group of a file or directory.
// If uid or gid are -1, that value is ignored. Symlinks are updated, not followed.
func (fs *Local) Lchown(path Path, uid, gid int) error {
	return osLchown(string(fs.root.Resolve(path)), uid, gid)
}

// Chtimes modifies the file or directory metadata for access and modification times.
func (fs *Local) Chtimes(path Path, atime time.Time, mtime time.Time) error {
	return os.Chtimes(string(fs.root.Resolve(path)), atime, mtime)
}
