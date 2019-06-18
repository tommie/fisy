package remote

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/tommie/fisy/remote/testutil"
)

var reconnectingSFTPClientIsACloseableSFTPClient CloseableSFTPClient = &ReconnectingSFTPClient{}

func TestReconnectingSFTPClientClose(t *testing.T) {
	mc := testutil.NewFakeSFTPClient("path")
	c, err := NewReconnectingSFTPClient(func() (CloseableSFTPClient, error) {
		return mc, nil
	})
	if err != nil {
		t.Fatalf("NewReconnectingSFTPClient failed: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Closed failed: %v", err)
	}

	if want := 1; mc.NClosed != want {
		t.Errorf("Close calls: got %v, want %v", mc.NClosed, want)
	}
	if _, err := c.StatVFS(""); err != ErrClientClosed {
		t.Errorf("StatVFS: got %v, want %v", err, ErrClientClosed)
	}
	if want := 0; mc.NCalls["StatVFS"] != want {
		t.Errorf("mc.StatVFS calls: got %v, want %v", mc.NCalls["StatVFS"], want)
	}
}

func TestReconnectingSFTPClientDoHandlesDialerError(t *testing.T) {
	wantErr := fmt.Errorf("fake error")
	mc := testutil.NewFakeSFTPClient("path")
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
	mc := testutil.NewFakeSFTPClient("path")
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
		{"Lstat", func(c SFTPClient) error { return isEqual(&testutil.FakeFileInfo{})(c.Lstat("path")) }},
		{"Mkdir", func(c SFTPClient) error { return c.Mkdir("path") }},
		{"Open", func(c SFTPClient) error { return isEqual(&sftp.File{})(c.Open("path")) }},
		{"PosixRename", func(c SFTPClient) error { return c.PosixRename("oldname", "newname") }},
		{"ReadDir", func(c SFTPClient) error { return isEqual([]os.FileInfo{&testutil.FakeFileInfo{}})(c.ReadDir("path")) }},
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

			mc := testutil.NewFakeSFTPClient("path")
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
			if want := 1; mc.NCalls[tst.Name] != want {
				t.Errorf("NCalls: got %v, want %v", mc.NCalls[tst.Name], want)
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
