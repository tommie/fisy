package fs

import (
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/sftp"
)

type SFTPClient interface {
	Chmod(path string, mode os.FileMode) error
	Chown(path string, uid, gid int) error
	Chtimes(path string, atime time.Time, mtime time.Time) error
	Create(path string) (*sftp.File, error)
	Link(oldname, newname string) error
	Lstat(p string) (os.FileInfo, error)
	Mkdir(path string) error
	Open(path string) (*sftp.File, error)
	PosixRename(oldname, newname string) error
	ReadDir(p string) ([]os.FileInfo, error)
	ReadLink(p string) (string, error)
	Remove(path string) error
	RemoveDirectory(path string) error
	Rename(oldname, newname string) error
	Stat(p string) (os.FileInfo, error)
	StatVFS(path string) (*sftp.StatVFS, error)
	Symlink(oldname, newname string) error
}

type RetryableSFTPClient interface {
	SFTPClient
	Close() error
}

type RetryingSFTPClient struct {
	dialer func() (RetryableSFTPClient, error)
	timeout time.Duration

	client    RetryableSFTPClient
	clientErr error
	mu        sync.Mutex
}

func NewRetryingSFTPClient(dialer func() (RetryableSFTPClient, error), timeout time.Duration) (*RetryingSFTPClient, error) {
	c := &RetryingSFTPClient{
		dialer: dialer,
		timeout: timeout,
	}

	// Check that the dialer works.
	_, err := c.getClient()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *RetryingSFTPClient) getClient() (RetryableSFTPClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil && c.clientErr == nil {
		c.client, c.clientErr = c.dialer()
	}

	return c.client, c.clientErr
}

func (c *RetryingSFTPClient) handleRetry(usedClient RetryableSFTPClient, err error) bool {
	switch err {
	case sftp.ErrSshFxConnectionLost, sftp.ErrSshFxNoConnection:
		c.mu.Lock()
		if c.client == usedClient {
			c.client = nil
			c.clientErr = nil
			c.mu.Unlock()
			usedClient.Close()
		} else {
			c.mu.Unlock()
		}
		return true

	default:
		return false
	}
}

func (c *RetryingSFTPClient) doIdempotent(fun func(SFTPClient) error) error {
	const maxDelay = 1 * time.Minute

	deadline := time.Now().Add(c.timeout)
	delay := 300 * time.Millisecond
	for {
		client, err := c.getClient()
		if err != nil {
			return err
		}

		err = fun(client)
		if err == nil {
			return nil
		}

		if !c.handleRetry(client, err) || time.Now().Add(delay).After(deadline) {
			// Not retriable.
			return err
		}

		glog.Warningf("Got retriable error (backoff %v, timeout %v): %v", delay, c.timeout, err)
		<-time.After(delay)

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

func (c *RetryingSFTPClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

func (c *RetryingSFTPClient) Chmod(path string, mode os.FileMode) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.Chmod(path, mode)
	})
}

func (c *RetryingSFTPClient) Chown(path string, uid, gid int) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.Chown(path, uid, gid)
	})
}

func (c *RetryingSFTPClient) Chtimes(path string, atime time.Time, mtime time.Time) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.Chtimes(path, atime, mtime)
	})
}

func (c *RetryingSFTPClient) Create(path string) (f *sftp.File, err error) {
	err = c.doIdempotent(func(client SFTPClient) error {
		f, err = client.Create(path)
		return err
	})
	return
}

func (c *RetryingSFTPClient) Link(oldname, newname string) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.Link(oldname, newname)
	})
}

func (c *RetryingSFTPClient) Lstat(p string) (fi os.FileInfo, err error) {
	err = c.doIdempotent(func(client SFTPClient) error {
		fi, err = client.Lstat(p)
		return err
	})
	return
}

func (c *RetryingSFTPClient) Mkdir(path string) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.Mkdir(path)
	})
}

func (c *RetryingSFTPClient) Open(path string) (f *sftp.File, err error) {
	err = c.doIdempotent(func(client SFTPClient) error {
		f, err = client.Open(path)
		return err
	})
	return
}

func (c *RetryingSFTPClient) PosixRename(oldname, newname string) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.PosixRename(oldname, newname)
	})
}

func (c *RetryingSFTPClient) ReadDir(p string) (fis []os.FileInfo, err error) {
	err = c.doIdempotent(func(client SFTPClient) error {
		fis, err = client.ReadDir(p)
		return err
	})
	return
}

func (c *RetryingSFTPClient) ReadLink(p string) (s string, err error) {
	err = c.doIdempotent(func(client SFTPClient) error {
		s, err = client.ReadLink(p)
		return err
	})
	return
}

func (c *RetryingSFTPClient) Remove(path string) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.Remove(path)
	})
}

func (c *RetryingSFTPClient) RemoveDirectory(path string) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.RemoveDirectory(path)
	})
}

func (c *RetryingSFTPClient) Rename(oldname, newname string) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.Rename(oldname, newname)
	})
}

func (c *RetryingSFTPClient) Stat(p string) (fi os.FileInfo, err error) {
	err = c.doIdempotent(func(client SFTPClient) error {
		fi, err = client.Stat(p)
		return err
	})
	return
}

func (c *RetryingSFTPClient) StatVFS(path string) (st *sftp.StatVFS, err error) {
	err = c.doIdempotent(func(client SFTPClient) error {
		st, err = client.StatVFS(path)
		return err
	})
	return
}

func (c *RetryingSFTPClient) Symlink(oldname, newname string) error {
	return c.doIdempotent(func(client SFTPClient) error {
		return client.Symlink(oldname, newname)
	})
}
