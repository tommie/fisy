package remote

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/pkg/sftp"
)

func TestIsRetriable(t *testing.T) {
	tsts := []struct {
		Name string
		Want bool
		Err  error
	}{
		{"nil", false, nil},
		{"os.ErrNotExist", false, os.ErrNotExist},

		{"SSHConnectionLost", true, sftp.ErrSshFxConnectionLost},
		{"SSHNoConnection", true, sftp.ErrSshFxNoConnection},
		{"InPathError", true, &os.PathError{Err: sftp.ErrSshFxConnectionLost}},
		{"InLinkError", true, &os.LinkError{Err: sftp.ErrSshFxConnectionLost}},
	}
	for _, tst := range tsts {
		tst := tst
		t.Run(tst.Name, func(t *testing.T) {
			got := IsRetriable(tst.Err)
			if got != tst.Want {
				t.Errorf("IsRetriable(%v): got %v, want %v", tst.Err, got, tst.Want)
			}
		})
	}
}

func TestIdempotentSucceeds(t *testing.T) {
	ctx := context.Background()

	var i int
	err := Idempotent(ctx, func() error {
		i++
		return nil
	})
	if err != nil {
		t.Errorf("Idempotent failed: %v", err)
	}

	if want := 1; i != want {
		t.Errorf("Idempotent calls: got %v, want %v", i, want)
	}
}

func TestIdempotentNotRetriable(t *testing.T) {
	ctx := context.Background()

	var i int
	err := Idempotent(ctx, func() error {
		i++
		return os.ErrNotExist
	})
	if want := os.ErrNotExist; err != want {
		t.Errorf("Idempotent error: got %v, want %v", err, want)
	}

	if want := 1; i != want {
		t.Errorf("Idempotent calls: got %v, want %v", i, want)
	}
}

func TestIdempotentRetriable(t *testing.T) {
	ctx := context.Background()

	var nafterCalls int
	timeAfter = func(d time.Duration) <-chan time.Time {
		nafterCalls++
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	defer func() {
		timeAfter = time.After
	}()

	var i int
	err := Idempotent(ctx, func() error {
		i++
		switch i {
		case 1:
			return sftp.ErrSshFxConnectionLost
		default:
			return nil
		}
	})
	if err != nil {
		t.Errorf("Idempotent failed: %v", err)
	}

	if want := 2; i != want {
		t.Errorf("Idempotent calls: got %v, want %v", i, want)
	}
	if want := 1; nafterCalls != want {
		t.Errorf("time.After calls: got %v, want %v", nafterCalls, want)
	}
}

func TestIdempotentDeadline(t *testing.T) {
	ctx, _ := context.WithDeadline(context.Background(), time.Now())

	var nafterCalls int
	timeAfter = func(d time.Duration) <-chan time.Time {
		nafterCalls++
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	defer func() {
		timeAfter = time.After
	}()

	var i int
	err := Idempotent(ctx, func() error {
		i++
		return sftp.ErrSshFxConnectionLost
	})
	if want := context.DeadlineExceeded; err != want {
		t.Errorf("Idempotent error: got %v, want %v", err, want)
	}

	if want := 1; i != want {
		t.Errorf("Idempotent calls: got %v, want %v", i, want)
	}
	if want := 1; nafterCalls != want {
		t.Errorf("time.After calls: got %v, want %v", nafterCalls, want)
	}
}
