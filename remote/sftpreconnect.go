package remote

import (
	"errors"
	"os"
	"sync"
	"time"

	"github.com/pkg/sftp"
)

// ErrClientClosed signals that Close was called, so no new operations
// could be started.
var ErrClientClosed = errors.New("client is closed")

// A CloseableSFTPClient can, in addition to SFTPClient operations,
// also be closed cleanly.
type CloseableSFTPClient interface {
	SFTPClient

	// Close releases resources used by this client.
	Close() error
}

// A ReconnectingSFTPClient will reconnect if a disconnect happens,
// but will not do retries on its own.
type ReconnectingSFTPClient struct {
	dialer func() (CloseableSFTPClient, error)

	client    CloseableSFTPClient
	clientErr error
	mu        sync.Mutex
}

// NewReconnectingSFTPClient creates a new client using the given
// dialer. An error is returned if the dialer couldn't create an
// initial client.
func NewReconnectingSFTPClient(dialer func() (CloseableSFTPClient, error)) (*ReconnectingSFTPClient, error) {
	c := &ReconnectingSFTPClient{
		dialer: dialer,
	}

	// Check that the dialer works.
	_, err := c.getClient()
	if err != nil {
		return nil, err
	}

	return c, nil
}

// getClient returns the existing client, or creates a new.
func (c *ReconnectingSFTPClient) getClient() (SFTPClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil && c.clientErr != ErrClientClosed {
		c.client, c.clientErr = c.dialer()
	}

	return c.client, c.clientErr
}

// handleError closes the client if there was a disconnection error.
func (c *ReconnectingSFTPClient) handleError(usedClient SFTPClient, err error) error {
	if !IsRetriable(err) {
		return nil
	}

	c.mu.Lock()
	if c.client == usedClient {
		cc := c.client
		c.client = nil
		c.clientErr = nil
		c.mu.Unlock()

		return cc.Close()
	} else {
		c.mu.Unlock()
	}
	return nil
}

// do runs a function with a client and closes the client if the
// function returns a disconnection error.
func (c *ReconnectingSFTPClient) do(fun func(SFTPClient) error) error {
	client, err := c.getClient()
	if err != nil {
		return err
	}

	err = fun(client)
	if err == nil {
		return nil
	}

	// We already have an error, so we don't report if handleErr
	// returns an error.
	c.handleError(client, err)
	return err
}

// Close closes the open client, and makes new operations fail with
// ErrClientClosed.
func (c *ReconnectingSFTPClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clientErr = ErrClientClosed
	if c.client != nil {
		cc := c.client
		c.client = nil
		return cc.Close()
	}
	return nil
}

func (c *ReconnectingSFTPClient) Chmod(path string, mode os.FileMode) error {
	return c.do(func(client SFTPClient) error {
		return client.Chmod(path, mode)
	})
}

func (c *ReconnectingSFTPClient) Chown(path string, uid, gid int) error {
	return c.do(func(client SFTPClient) error {
		return client.Chown(path, uid, gid)
	})
}

func (c *ReconnectingSFTPClient) Chtimes(path string, atime time.Time, mtime time.Time) error {
	return c.do(func(client SFTPClient) error {
		return client.Chtimes(path, atime, mtime)
	})
}

func (c *ReconnectingSFTPClient) Create(path string) (f *sftp.File, err error) {
	err = c.do(func(client SFTPClient) error {
		f, err = client.Create(path)
		return err
	})
	return
}

func (c *ReconnectingSFTPClient) Link(oldname, newname string) error {
	return c.do(func(client SFTPClient) error {
		return client.Link(oldname, newname)
	})
}

func (c *ReconnectingSFTPClient) Lstat(path string) (fi os.FileInfo, err error) {
	err = c.do(func(client SFTPClient) error {
		fi, err = client.Lstat(path)
		return err
	})
	return
}

func (c *ReconnectingSFTPClient) Mkdir(path string) error {
	return c.do(func(client SFTPClient) error {
		return client.Mkdir(path)
	})
}

func (c *ReconnectingSFTPClient) Open(path string) (f *sftp.File, err error) {
	err = c.do(func(client SFTPClient) error {
		f, err = client.Open(path)
		return err
	})
	return
}

func (c *ReconnectingSFTPClient) PosixRename(oldname, newname string) error {
	return c.do(func(client SFTPClient) error {
		return client.PosixRename(oldname, newname)
	})
}

func (c *ReconnectingSFTPClient) ReadDir(path string) (fis []os.FileInfo, err error) {
	err = c.do(func(client SFTPClient) error {
		fis, err = client.ReadDir(path)
		return err
	})
	return
}

func (c *ReconnectingSFTPClient) ReadLink(path string) (s string, err error) {
	err = c.do(func(client SFTPClient) error {
		s, err = client.ReadLink(path)
		return err
	})
	return
}

func (c *ReconnectingSFTPClient) Remove(path string) error {
	return c.do(func(client SFTPClient) error {
		return client.Remove(path)
	})
}

func (c *ReconnectingSFTPClient) RemoveDirectory(path string) error {
	return c.do(func(client SFTPClient) error {
		return client.RemoveDirectory(path)
	})
}

func (c *ReconnectingSFTPClient) StatVFS(path string) (st *sftp.StatVFS, err error) {
	err = c.do(func(client SFTPClient) error {
		st, err = client.StatVFS(path)
		return err
	})
	return
}

func (c *ReconnectingSFTPClient) Symlink(oldname, newname string) error {
	return c.do(func(client SFTPClient) error {
		return client.Symlink(oldname, newname)
	})
}
