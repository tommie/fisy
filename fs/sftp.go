package fs

import (
	"context"
	"os"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type SFTPFileSystem struct {
	client SFTPClient
	root   Path
}

func NewSFTPFileSystem(client SFTPClient, root Path) *SFTPFileSystem {
	return &SFTPFileSystem{
		client: client,
		root:   root,
	}
}

func (fs *SFTPFileSystem) Open(path Path) (FileReader, error) {
	p := string(fs.root.Resolve(path))
	f, err := fs.client.Open(p)
	if err != nil {
		return nil, &os.PathError{Op: "sftp:open", Path: p, Err: err}
	}
	return &sftpFileReader{f, fs.client}, nil
}

type sftpFileReader struct {
	*sftp.File

	client SFTPClient
}

func (fr *sftpFileReader) Readdir() ([]os.FileInfo, error) {
	fis, err := fr.client.ReadDir(fr.File.Name())
	if err != nil {
		return nil, &os.PathError{Op: "sftp:readdir", Path: fr.File.Name(), Err: err}
	}
	return fis, nil
}

func (fs *SFTPFileSystem) Readlink(path Path) (Path, error) {
	p := string(fs.root.Resolve(path))
	linkdest, err := fs.client.ReadLink(p)
	if err != nil {
		return "", &os.PathError{Op: "sftp:readlink", Path: p, Err: err}
	}
	return Path(linkdest), nil
}

func (fs *SFTPFileSystem) Create(path Path) (FileWriter, error) {
	p := string(fs.root.Resolve(path))
	f, err := fs.client.Create(p)
	if err != nil {
		return nil, &os.PathError{Op: "sftp:create", Path: p, Err: err}
	}
	return &sftpFileWriter{f, fs.client}, nil
}

type sftpFileWriter struct {
	*sftp.File

	client SFTPClient
}

func (fw *sftpFileWriter) Chtimes(atime time.Time, mtime time.Time) error {
	return fw.client.Chtimes(fw.File.Name(), atime, mtime)
}

func (fs *SFTPFileSystem) Keep(path Path) error {
	return nil
}

func (fs *SFTPFileSystem) Mkdir(path Path, mode os.FileMode, uid, gid int) error {
	p := string(fs.root.Resolve(path))
	if err := fs.client.Mkdir(p); err != nil {
		if IsExist(err) {
			return &os.PathError{Op: "sftp:mkdir", Path: p, Err: err}
		}
		// openssh sftp-server.c doesn't return FILE_ALREADY_EXISTS.
		if _, err := fs.client.Lstat(p); err != nil {
			return &os.PathError{Op: "sftp:lstat", Path: p, Err: err}
		}
		return &os.PathError{Op: "sftp:mkdir", Path: p, Err: os.ErrExist}
	}
	if err := fs.client.Chmod(p, mode); err != nil {
		fs.client.RemoveDirectory(p)
		return &os.PathError{Op: "sftp:chmod", Path: p, Err: err}
	}
	if err := fs.client.Chown(p, uid, gid); err != nil {
		fs.client.RemoveDirectory(p)
		return &os.PathError{Op: "sftp:chown", Path: p, Err: err}
	}
	return nil
}

func (fs *SFTPFileSystem) Link(oldpath Path, newpath Path) error {
	oldp, newp := string(fs.root.Resolve(oldpath)), string(fs.root.Resolve(newpath))
	if err := fs.client.Link(oldp, newp); err != nil {
		return &os.LinkError{Op: "sftp:link", Old: oldp, New: newp, Err: err}
	}
	return nil
}

func (fs *SFTPFileSystem) Symlink(oldpath Path, newpath Path) error {
	oldp, newp := string(oldpath), string(fs.root.Resolve(newpath))
	if err := fs.client.Symlink(oldp, newp); err != nil {
		return &os.LinkError{Op: "sftp:symlink", Old: oldp, New: newp, Err: err}
	}
	return nil
}

func (fs *SFTPFileSystem) Rename(oldpath Path, newpath Path) error {
	oldp, newp := string(fs.root.Resolve(oldpath)), string(fs.root.Resolve(newpath))
	if err := fs.client.PosixRename(oldp, newp); err != nil {
		return &os.LinkError{Op: "sftp:rename", Old: oldp, New: newp, Err: err}
	}
	return nil
}

func (fs *SFTPFileSystem) RemoveAll(path Path) error {
	return fs.removeAll(context.Background(), path, semaphore.NewWeighted(64))
}

func (fs *SFTPFileSystem) removeAll(ctx context.Context, path Path, sem *semaphore.Weighted) error {
	readdir := func() ([]os.FileInfo, error) {
		if err := sem.Acquire(ctx, 1); err != nil {
			return nil, err
		}
		defer sem.Release(1)

		return fs.client.ReadDir(string(fs.root.Resolve(path)))
	}
	remove := func(path Path) error {
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}
		defer sem.Release(1)

		return fs.Remove(path)
	}

	// We expect most callers to use RemoveAll with a directory, so optimize for that.
	fis, err := readdir()
	if err != nil {
		// It was probably a file.
		return remove(path)
	}

	var eg errgroup.Group
	for _, fi := range fis {
		fi := fi
		eg.Go(func() error {
			if fi.IsDir() {
				return fs.removeAll(ctx, path.Resolve(Path(fi.Name())), sem)
			}

			return remove(path.Resolve(Path(fi.Name())))
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	return remove(path)
}

func (fs *SFTPFileSystem) Remove(path Path) error {
	p := string(fs.root.Resolve(path))
	if err := fs.client.Remove(p); err != nil {
		return &os.PathError{Op: "sftp:remove", Path: p, Err: err}
	}
	return nil
}

func (fs *SFTPFileSystem) Stat() (FSInfo, error) {
	sf, err := fs.client.StatVFS(string(fs.root))
	if err != nil {
		return FSInfo{}, err
	}
	return FSInfo{FreeSpace: sf.Frsize * sf.Bavail}, nil
}

func (fs *SFTPFileSystem) Chmod(path Path, mode os.FileMode) error {
	p := string(fs.root.Resolve(path))
	if err := fs.client.Chmod(p, mode); err != nil {
		return &os.PathError{Op: "sftp:chmod", Path: p, Err: err}
	}
	return nil
}

func (fs *SFTPFileSystem) Lchown(path Path, uid, gid int) error {
	p := string(fs.root.Resolve(path))
	if err := fs.client.Chown(p, uid, gid); err != nil {
		return &os.PathError{Op: "sftp:lchown", Path: p, Err: err}
	}
	return nil
}

func (fs *SFTPFileSystem) Chtimes(path Path, atime time.Time, mtime time.Time) error {
	p := string(fs.root.Resolve(path))
	if err := fs.client.Chtimes(p, atime, mtime); err != nil {
		return &os.PathError{Op: "sftp:chtimes", Path: p, Err: err}
	}
	return nil
}
