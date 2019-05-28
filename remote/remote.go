// Package remote contains helpers for network operations.
package remote

import (
	"context"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/sftp"
)

// An SFTPClient abstracts github.com/pkg/sftp.Client.
type SFTPClient interface {
	Chmod(path string, mode os.FileMode) error
	Chown(path string, uid, gid int) error
	Chtimes(path string, atime time.Time, mtime time.Time) error
	Create(path string) (*sftp.File, error)
	Link(oldname, newname string) error
	Lstat(path string) (os.FileInfo, error)
	Mkdir(path string) error
	Open(path string) (*sftp.File, error)
	PosixRename(oldname, newname string) error
	ReadDir(path string) ([]os.FileInfo, error)
	ReadLink(path string) (string, error)
	Remove(path string) error
	RemoveDirectory(path string) error
	StatVFS(path string) (*sftp.StatVFS, error)
	Symlink(oldname, newname string) error
}

// Returns whether it makes sense to retry on this kind of error.
func IsRetriable(err error) bool {
	if eerr, ok := err.(*os.LinkError); ok {
		err = eerr.Err
	}
	if eerr, ok := err.(*os.PathError); ok {
		err = eerr.Err
	}

	switch err {
	case sftp.ErrSshFxConnectionLost, sftp.ErrSshFxNoConnection:
		return true
	default:
		return false
	}
}

var timeAfter = time.After

// Idempotent retries an idempotent function until it succeeds or
// returns a non-retriable error. If the context is cancelled, the
// function will return during a back-off.
func Idempotent(ctx context.Context, fun func() error) error {
	const initialDelay = 300 * time.Millisecond
	const maxDelay = 1 * time.Minute

	delay := initialDelay
	for {
		err := fun()
		if err == nil {
			return nil
		}

		if !IsRetriable(err) {
			return err
		}

		if t, ok := ctx.Deadline(); ok {
			glog.Warningf("Got retriable error (backoff %v, deadline %v): %v", delay, t, err)
		} else {
			glog.Warningf("Got retriable error (backoff %v, no deadline): %v", delay, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeAfter(delay):
			// Continue.
		}

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

}
