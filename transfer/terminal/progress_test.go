package terminal

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/tommie/fisy/transfer"
)

func TestNewProgress(t *testing.T) {
	t.Run("outIsNotFile", func(t *testing.T) {
		var out bytes.Buffer
		p := NewProgress(&out, 10*time.Millisecond)

		if want := 0; p.width != want {
			t.Errorf("NewProgress width: got %v, want %v", p.width, want)
		}
	})

	t.Run("outIsNotTerminal", func(t *testing.T) {
		tmpf, err := ioutil.TempFile("", "progress_test-")
		if err != nil {
			t.Fatalf("TempFile failed: %v", err)
		}
		defer tmpf.Close()
		os.Remove(tmpf.Name())

		p := NewProgress(tmpf, 10*time.Millisecond)

		if want := 0; p.width != want {
			t.Errorf("NewProgress width: got %v, want %v", p.width, want)
		}
	})

	t.Run("outIsTerminal", func(t *testing.T) {
		origIsTerminal := isTerminal
		isTerminal = func(int) bool { return true }
		defer func() {
			isTerminal = origIsTerminal
		}()
		origGetSize := terminalGetSize
		terminalGetSize = func(int) (int, int, error) { return 123, 456, nil }
		defer func() {
			terminalGetSize = origGetSize
		}()

		tmpf, err := ioutil.TempFile("", "progress_test-")
		if err != nil {
			t.Fatalf("TempFile failed: %v", err)
		}
		defer tmpf.Close()
		os.Remove(tmpf.Name())

		p := NewProgress(tmpf, 10*time.Millisecond)

		if want := 123 - 1; p.width != want {
			t.Errorf("NewProgress width: got %v, want %v", p.width, want)
		}
	})

	t.Run("defaultTo80", func(t *testing.T) {
		origIsTerminal := isTerminal
		isTerminal = func(int) bool { return true }
		defer func() {
			isTerminal = origIsTerminal
		}()
		origGetSize := terminalGetSize
		terminalGetSize = func(int) (int, int, error) { return 0, 0, fmt.Errorf("mocked") }
		defer func() {
			terminalGetSize = origGetSize
		}()

		tmpf, err := ioutil.TempFile("", "progress_test-")
		if err != nil {
			t.Fatalf("TempFile failed: %v", err)
		}
		defer tmpf.Close()
		os.Remove(tmpf.Name())

		p := NewProgress(tmpf, 10*time.Millisecond)

		if want := 80 - 1; p.width != want {
			t.Errorf("NewProgress width: got %v, want %v", p.width, want)
		}
	})
}

func TestProgressRunUpload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &cancelWriter{cancel: cancel}
	p := &Progress{
		w:      out,
		period: 10 * time.Millisecond,
		width:  80,
	}
	var u fakeUpload
	p.RunUpload(ctx, &u)

	if want := 1; u.nstatsCalls != want {
		t.Errorf("nstatsCalls: got %v, want %v", u.nstatsCalls, want)
	}
	if out.Buffer.Len() < 10 {
		t.Errorf("out Len: got %v, want >= %v", out.Buffer.Len(), 10)
	}
}

func TestProgressFormatUploadStats(t *testing.T) {
	var out bytes.Buffer
	p := &Progress{
		w:      &out,
		period: 10 * time.Millisecond,
		// Short enough to cut the string.
		width: 26,
	}

	us := transfer.UploadStats{
		ProcessStats: transfer.ProcessStats{
			SourceFiles: 1,
			InProgress:  3,
		},
		UploadedBytes: 2,
		InodeTable:    4,
	}
	us.SetLast("test", &fakeFileInfo{}, nil)
	got := p.formatUploadStats(&us)

	if want := "    1 /  +2.0 B / 3: R tes"; got != want {
		t.Errorf("formatUploadStats: got %q, want %q", got, want)
	}
}

func TestProgressFinishUpload(t *testing.T) {
	timeNow = func() time.Time {
		return time.Date(2006, 2, 3, 15, 4, 5, 0, time.Local)
	}
	defer func() {
		timeNow = time.Now
	}()

	var out bytes.Buffer
	p := &Progress{
		w:      &out,
		period: 10 * time.Millisecond,
		// Short enough to cut the string.
		width: 30,
		start: timeNow().Add(-1 * time.Minute),
	}

	p.FinishUpload(&fakeUpload{})

	if want := "All done in 1m0s. Uploaded 1.0 B in 2 file(s). Kept 3.0 B in 4 file(s).\n"; out.String() != want {
		t.Errorf("out: got %q, want %q", out.String(), want)
	}
}

func TestStorageBytes(t *testing.T) {
	tsts := []struct {
		V    uint64
		Want string
	}{
		{0, "0 B"},
		{1, "1.0 B"},
		{500, "500 B"},
		{1024, "1.0 kiB"},
		{500 * 1024, "500 kiB"},
		{2 * 1024 * 1024, "2.0 MiB"},
		{3 * 1024 * 1024 * 1024, "3.0 GiB"},
		{4 * 1024 * 1024 * 1024 * 1024, "4.0 TiB"},
		{5 * 1024 * 1024 * 1024 * 1024 * 1024, "5.0 PiB"},
		{6 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024, "6.0 EiB"},
	}
	for _, tst := range tsts {
		t.Run(tst.Want, func(t *testing.T) {
			got := storageBytes(tst.V)
			if got != tst.Want {
				t.Errorf("storageBytes(%v): got %q, want %q", tst.V, got, tst.Want)
			}
		})
	}
}

type cancelWriter struct {
	bytes.Buffer
	cancel func()
}

func (w *cancelWriter) Write(bs []byte) (int, error) {
	w.cancel()
	return w.Buffer.Write(bs)
}

type fakeUpload struct {
	nstatsCalls int
}

func (u *fakeUpload) Stats() transfer.UploadStats {
	u.nstatsCalls++
	us := transfer.UploadStats{
		UploadedBytes: 1,
		UploadedFiles: 2,
		KeptBytes:     3,
		KeptFiles:     4,
	}
	us.SetLast("test", nil, nil)
	return us
}

type fakeFileInfo struct {
	os.FileInfo
}
