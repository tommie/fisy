package testutil

import (
	"os"
	"fmt"
	"time"

	"github.com/pkg/sftp"
)

type FakeFileInfo struct {
	os.FileInfo
}

type FakeSFTPClient struct {
	NClosed int
	NCalls  map[string]int
}

func NewFakeSFTPClient() *FakeSFTPClient {
	return &FakeSFTPClient{
		NCalls: map[string]int{},
	}
}

func (c *FakeSFTPClient) Close() error {
	c.NClosed++
	return nil
}

func (c *FakeSFTPClient) Chmod(path string, mode os.FileMode) error {
	c.NCalls["Chmod"]++
	if path != "path" || mode != 0654 {
		return fmt.Errorf("unexpected parameters to Chmod: %q, 0%o", path, mode)
	}
	return nil
}

func (c *FakeSFTPClient) Chown(path string, uid, gid int) error {
	c.NCalls["Chown"]++
	if path != "path" || uid != 42 || gid != 43 {
		return fmt.Errorf("unexpected parameters to Chown: %q, %v, %v", path, uid, gid)
	}
	return nil
}

func (c *FakeSFTPClient) Chtimes(path string, atime time.Time, mtime time.Time) error {
	c.NCalls["Chtimes"]++
	if path != "path" || !atime.Equal(time.Unix(42, 0)) || !mtime.Equal(time.Unix(43, 0)) {
		return fmt.Errorf("unexpected parameters to Chtimes: %q, %v, %v", path, atime, mtime)
	}
	return nil
}

func (c *FakeSFTPClient) Create(path string) (*sftp.File, error) {
	c.NCalls["Create"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to Create: %q", path)
	}
	return &sftp.File{}, nil
}

func (c *FakeSFTPClient) Link(oldname, newname string) error {
	c.NCalls["Link"]++
	if oldname != "oldname" || newname != "newname" {
		return fmt.Errorf("unexpected parameters to Link: %q, %q", oldname, newname)
	}
	return nil
}

func (c *FakeSFTPClient) Lstat(path string) (os.FileInfo, error) {
	c.NCalls["Lstat"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to Lstat: %q", path)
	}
	return &FakeFileInfo{}, nil
}

func (c *FakeSFTPClient) Mkdir(path string) error {
	c.NCalls["Mkdir"]++
	if path != "path" {
		return fmt.Errorf("unexpected parameters to Mkdir: %q", path)
	}
	return nil
}

func (c *FakeSFTPClient) Open(path string) (*sftp.File, error) {
	c.NCalls["Open"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to Open: %q", path)
	}
	return &sftp.File{}, nil
}

func (c *FakeSFTPClient) PosixRename(oldname, newname string) error {
	c.NCalls["PosixRename"]++
	if oldname != "oldname" || newname != "newname" {
		return fmt.Errorf("unexpected parameters to PosixRename: %q, %q", oldname, newname)
	}
	return nil
}

func (c *FakeSFTPClient) ReadDir(path string) ([]os.FileInfo, error) {
	c.NCalls["ReadDir"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to ReadDir: %q", path)
	}
	return []os.FileInfo{&FakeFileInfo{}}, nil
}

func (c *FakeSFTPClient) ReadLink(path string) (string, error) {
	c.NCalls["ReadLink"]++
	if path != "path" {
		return "", fmt.Errorf("unexpected parameters to ReadLink: %q", path)
	}
	return "dest", nil
}

func (c *FakeSFTPClient) Remove(path string) error {
	c.NCalls["Remove"]++
	if path != "path" {
		return fmt.Errorf("unexpected parameters to Remove: %q", path)
	}
	return nil
}

func (c *FakeSFTPClient) RemoveDirectory(path string) error {
	c.NCalls["RemoveDirectory"]++
	if path != "path" {
		return fmt.Errorf("unexpected parameters to RemoveDirectory: %q", path)
	}
	return nil
}

func (c *FakeSFTPClient) StatVFS(path string) (*sftp.StatVFS, error) {
	c.NCalls["StatVFS"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to StatVFS: %q", path)
	}
	return &sftp.StatVFS{}, nil
}

func (c *FakeSFTPClient) Symlink(oldname, newname string) error {
	c.NCalls["Symlink"]++
	if oldname != "oldname" || newname != "newname" {
		return fmt.Errorf("unexpected parameters to PosixRename: %q, %q", oldname, newname)
	}
	return nil
}
