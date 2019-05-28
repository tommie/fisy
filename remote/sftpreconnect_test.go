package remote

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/sftp"
)

var reconnectingSFTPClientIsACloseableSFTPClient CloseableSFTPClient = &ReconnectingSFTPClient{}

func TestReconnectingSFTPClientClose(t *testing.T) {
	mc := newFakeSFTPClient()
	c, err := NewReconnectingSFTPClient(func() (CloseableSFTPClient, error) {
		return mc, nil
	})
	if err != nil {
		t.Fatalf("NewReconnectingSFTPClient failed: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Closed failed: %v", err)
	}

	if want := 1; mc.nclosed != want {
		t.Errorf("Close calls: got %v, want %v", mc.nclosed, want)
	}
	if _, err := c.StatVFS(""); err != ErrClientClosed {
		t.Errorf("StatVFS: got %v, want %v", err, ErrClientClosed)
	}
	if want := 0; mc.ncalls["StatVFS"] != want {
		t.Errorf("mc.StatVFS calls: got %v, want %v", mc.ncalls["StatVFS"], want)
	}
}

func TestReconnectingSFTPClientDoHandlesDialerError(t *testing.T) {
	wantErr := fmt.Errorf("fake error")
	mc := newFakeSFTPClient()
	var i int
	c, err := NewReconnectingSFTPClient(func() (CloseableSFTPClient, error) {
		i++
		switch i {
		case 1:
			return mc, nil
		default:
			return nil, wantErr
		}
	})
	if err != nil {
		t.Fatalf("NewReconnectingSFTPClient failed: %v", err)
	}
	defer c.Close()

	c.handleError(mc, sftp.ErrSshFxConnectionLost)

	if err := c.do(nil); err != wantErr {
		t.Errorf("do error: got %v, want %v", err, wantErr)
	}

	if err := c.do(nil); err != wantErr {
		t.Errorf("do error: got %v, want %v", err, wantErr)
	}

	if want := 3; i != want {
		t.Errorf("dialer calls: got %v, want %v", i, want)
	}
}

func TestReconnectingSFTPClientDoesntReconnectAfterClose(t *testing.T) {
	mc := newFakeSFTPClient()
	var i int
	c, err := NewReconnectingSFTPClient(func() (CloseableSFTPClient, error) {
		i++
		return mc, nil
	})
	if err != nil {
		t.Fatalf("NewReconnectingSFTPClient failed: %v", err)
	}
	c.Close()

	c.handleError(mc, sftp.ErrSshFxConnectionLost)

	if _, err := c.getClient(); err != ErrClientClosed {
		t.Errorf("getClient error: got %v, want %v", err, ErrClientClosed)
	}

	if want := 1; i != want {
		t.Errorf("dialer calls: got %v, want %v", i, want)
	}
}

func TestReconnectingSFTPClientOps(t *testing.T) {
	tsts := []struct {
		Name string
		Fun  func(SFTPClient) error
	}{
		{"Chmod", func(c SFTPClient) error { return c.Chmod("path", 0654) }},
		{"Chown", func(c SFTPClient) error { return c.Chown("path", 42, 43) }},
		{"Chtimes", func(c SFTPClient) error { return c.Chtimes("path", time.Unix(42, 0), time.Unix(43, 0)) }},
		{"Create", func(c SFTPClient) error { return isEqual(&sftp.File{})(c.Create("path")) }},
		{"Link", func(c SFTPClient) error { return c.Link("oldname", "newname") }},
		{"Lstat", func(c SFTPClient) error { return isEqual(&fakeFileInfo{})(c.Lstat("path")) }},
		{"Mkdir", func(c SFTPClient) error { return c.Mkdir("path") }},
		{"Open", func(c SFTPClient) error { return isEqual(&sftp.File{})(c.Open("path")) }},
		{"PosixRename", func(c SFTPClient) error { return c.PosixRename("oldname", "newname") }},
		{"ReadDir", func(c SFTPClient) error { return isEqual([]os.FileInfo{&fakeFileInfo{}})(c.ReadDir("path")) }},
		{"ReadLink", func(c SFTPClient) error { return isEqual("dest")(c.ReadLink("path")) }},
		{"Remove", func(c SFTPClient) error { return c.Remove("path") }},
		{"RemoveDirectory", func(c SFTPClient) error { return c.RemoveDirectory("path") }},
		{"StatVFS", func(c SFTPClient) error { return isEqual(&sftp.StatVFS{})(c.StatVFS("path")) }},
		{"Symlink", func(c SFTPClient) error { return c.Symlink("oldname", "newname") }},
	}
	for _, tst := range tsts {
		tst := tst
		t.Run(tst.Name, func(t *testing.T) {
			t.Parallel()

			mc := newFakeSFTPClient()
			c, err := NewReconnectingSFTPClient(func() (CloseableSFTPClient, error) {
				return mc, nil
			})
			if err != nil {
				t.Fatalf("NewReconnectingSFTPClient failed: %v", err)
			}
			defer c.Close()

			if err := tst.Fun(c); err != nil {
				t.Errorf("%v", err)
			}
			if want := 1; mc.ncalls[tst.Name] != want {
				t.Errorf("ncalls: got %v, want %v", mc.ncalls[tst.Name], want)
			}
		})
	}
}

func isEqual(want interface{}) func(got interface{}, err error) error {
	return func(got interface{}, err error) error {
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(want, got) {
			return fmt.Errorf("return value: got %#v, want %#v", got, want)
		}
		return nil
	}
}

type fakeFileInfo struct {
	os.FileInfo
}

type fakeSFTPClient struct {
	CloseableSFTPClient

	nclosed int
	ncalls  map[string]int
}

func newFakeSFTPClient() *fakeSFTPClient {
	return &fakeSFTPClient{
		ncalls: map[string]int{},
	}
}

func (c *fakeSFTPClient) Close() error {
	c.nclosed++
	return nil
}

func (c *fakeSFTPClient) Chmod(path string, mode os.FileMode) error {
	c.ncalls["Chmod"]++
	if path != "path" || mode != 0654 {
		return fmt.Errorf("unexpected parameters to Chmod: %q, 0%o", path, mode)
	}
	return nil
}

func (c *fakeSFTPClient) Chown(path string, uid, gid int) error {
	c.ncalls["Chown"]++
	if path != "path" || uid != 42 || gid != 43 {
		return fmt.Errorf("unexpected parameters to Chown: %q, %v, %v", path, uid, gid)
	}
	return nil
}

func (c *fakeSFTPClient) Chtimes(path string, atime time.Time, mtime time.Time) error {
	c.ncalls["Chtimes"]++
	if path != "path" || !atime.Equal(time.Unix(42, 0)) || !mtime.Equal(time.Unix(43, 0)) {
		return fmt.Errorf("unexpected parameters to Chtimes: %q, %v, %v", path, atime, mtime)
	}
	return nil
}

func (c *fakeSFTPClient) Create(path string) (*sftp.File, error) {
	c.ncalls["Create"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to Create: %q", path)
	}
	return &sftp.File{}, nil
}

func (c *fakeSFTPClient) Link(oldname, newname string) error {
	c.ncalls["Link"]++
	if oldname != "oldname" || newname != "newname" {
		return fmt.Errorf("unexpected parameters to Link: %q, %q", oldname, newname)
	}
	return nil
}

func (c *fakeSFTPClient) Lstat(path string) (os.FileInfo, error) {
	c.ncalls["Lstat"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to Lstat: %q", path)
	}
	return &fakeFileInfo{}, nil
}

func (c *fakeSFTPClient) Mkdir(path string) error {
	c.ncalls["Mkdir"]++
	if path != "path" {
		return fmt.Errorf("unexpected parameters to Mkdir: %q", path)
	}
	return nil
}

func (c *fakeSFTPClient) Open(path string) (*sftp.File, error) {
	c.ncalls["Open"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to Open: %q", path)
	}
	return &sftp.File{}, nil
}

func (c *fakeSFTPClient) PosixRename(oldname, newname string) error {
	c.ncalls["PosixRename"]++
	if oldname != "oldname" || newname != "newname" {
		return fmt.Errorf("unexpected parameters to PosixRename: %q, %q", oldname, newname)
	}
	return nil
}

func (c *fakeSFTPClient) ReadDir(path string) ([]os.FileInfo, error) {
	c.ncalls["ReadDir"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to ReadDir: %q", path)
	}
	return []os.FileInfo{&fakeFileInfo{}}, nil
}

func (c *fakeSFTPClient) ReadLink(path string) (string, error) {
	c.ncalls["ReadLink"]++
	if path != "path" {
		return "", fmt.Errorf("unexpected parameters to ReadLink: %q", path)
	}
	return "dest", nil
}

func (c *fakeSFTPClient) Remove(path string) error {
	c.ncalls["Remove"]++
	if path != "path" {
		return fmt.Errorf("unexpected parameters to Remove: %q", path)
	}
	return nil
}

func (c *fakeSFTPClient) RemoveDirectory(path string) error {
	c.ncalls["RemoveDirectory"]++
	if path != "path" {
		return fmt.Errorf("unexpected parameters to RemoveDirectory: %q", path)
	}
	return nil
}

func (c *fakeSFTPClient) StatVFS(path string) (*sftp.StatVFS, error) {
	c.ncalls["StatVFS"]++
	if path != "path" {
		return nil, fmt.Errorf("unexpected parameters to StatVFS: %q", path)
	}
	return &sftp.StatVFS{}, nil
}

func (c *fakeSFTPClient) Symlink(oldname, newname string) error {
	c.ncalls["Symlink"]++
	if oldname != "oldname" || newname != "newname" {
		return fmt.Errorf("unexpected parameters to PosixRename: %q, %q", oldname, newname)
	}
	return nil
}
