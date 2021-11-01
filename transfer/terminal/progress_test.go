package terminal

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/vbauerster/mpb/v7"

	"github.com/tommie/fisy/transfer"
)

func TestNewProgress(t *testing.T) {
	t.Run("outIsNotFile", func(t *testing.T) {
		var out bytes.Buffer
		p := NewProgress(&out, 10*time.Millisecond)

		if _, ok := p.(NoOpProgress); !ok {
			t.Errorf("NewProgress: got a %T, want a NoOpProgress", p)
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

		if _, ok := p.(NoOpProgress); !ok {
			t.Errorf("NewProgress: got a %T, want a NoOpProgress", p)
		}
	})
}

func TestTerminalProgressFileHook(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		var p terminalProgress
		var nbytes uint64
		p.FileHook(&fakeFileInfo{}, transfer.Create, &nbytes, transfer.InProgress)

		v, ok := p.inProgress.Load("file")
		if !ok {
			t.Errorf("FileHook: got %v, want non-nil", v)
		}
		tf := v.(*terminalFile)
		if tf.uploadedBytes != &nbytes {
			t.Errorf("FileHook uploadedBytes: got %#v, want %#v", tf.uploadedBytes, &nbytes)
		}
	})

	t.Run("delete", func(t *testing.T) {
		var p terminalProgress
		var nbytes uint64
		p.inProgress.Store("file", &terminalFile{})
		p.FileHook(&fakeFileInfo{}, transfer.Create, &nbytes, nil)

		v, ok := p.inProgress.Load("file")
		if ok {
			t.Errorf("FileHook: got %v, want nil", v)
		}
	})
}

func TestTerminalProgressRunUpload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	p := &terminalProgress{
		period: 10 * time.Millisecond,
	}
	mp := &fakeBarContainer{Progress: mpb.NewWithContext(ctx, mpb.WithOutput(nil))}
	var u fakeUpload

	ructx, rucancel := context.WithCancel(ctx)

	// All important code will run once before the context is checked.
	rucancel()

	p.runUpload(ructx, mp, &u)

	if want := 1; u.nstatsCalls != want {
		t.Errorf("nstatsCalls: got %v, want %v", u.nstatsCalls, want)
	}
	if got, want := len(mp.bars), 3; got != want {
		t.Errorf("BarCount: got %v, want %v", got, want)
	}
	if got, want := mp.bars[0].Current(), int64(3); got != want {
		t.Errorf("bytesSp Current: got %v, want %v", got, want)
	}
	if got, want := mp.bars[1].Current(), int64(2); got != want {
		t.Errorf("filesSp Current: got %v, want %v", got, want)
	}
	if got, want := mp.bars[2].Current(), int64(1); got != want {
		t.Errorf("inProgBar Current: got %v, want %v", got, want)
	}

	cancel()
	mp.Wait()
}

func TestTerminalProgressUpdateInProgBars(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		var p terminalProgress

		v := uint64(42)
		p.FileHook(&fakeFileInfo{}, transfer.Create, &v, transfer.InProgress)

		mp := &fakeBarContainer{Progress: mpb.NewWithContext(ctx, mpb.WithOutput(nil))}
		inProgBars := map[string]*mpb.Bar{}
		got := p.updateInProgBars(inProgBars, mp, 1)

		if got, want := len(mp.bars), 1; got != want {
			t.Errorf("BarCount: got %v, want %v", got, want)
		}
		if want := map[string]*mpb.Bar{"file": mp.bars[0]}; !reflect.DeepEqual(got, want) {
			t.Errorf("updateInProgBars: got %+v, want %+v", got, want)
		}
		if got, want := got["file"].Current(), int64(42); got != want {
			t.Errorf("Current: got %v, want %v", got, want)
		}

		cancel()
		mp.Wait()
	})

	t.Run("delete", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		var p terminalProgress
		mp := &fakeBarContainer{Progress: mpb.NewWithContext(ctx, mpb.WithOutput(nil))}
		inProgBars := map[string]*mpb.Bar{
			"donefile": mp.Progress.AddBar(0),
		}
		got := p.updateInProgBars(inProgBars, mp, 1)

		if got, want := len(mp.bars), 0; got != want {
			t.Errorf("BarCount: got %v, want %v", got, want)
		}
		if want := map[string]*mpb.Bar{}; !reflect.DeepEqual(got, want) {
			t.Errorf("updateInProgBars: got %+v, want %+v", got, want)
		}

		cancel()
		mp.Wait()
	})

	t.Run("update", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		var p terminalProgress

		v := uint64(42)
		p.FileHook(&fakeFileInfo{}, transfer.Create, &v, transfer.InProgress)

		mp := &fakeBarContainer{Progress: mpb.NewWithContext(ctx, mpb.WithOutput(nil))}
		inProgBars := map[string]*mpb.Bar{
			"file": mp.Progress.AddBar(0),
		}
		got := p.updateInProgBars(inProgBars, mp, 1)

		if got, want := got["file"].Current(), int64(42); got != want {
			t.Errorf("Current: got %v, want %v", got, want)
		}

		cancel()
		mp.Wait()
	})

	t.Run("rankSize", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		var p terminalProgress

		p.FileHook(&fakeFileInfo{name: "file1", size: 42}, transfer.Create, new(uint64), transfer.InProgress)
		p.FileHook(&fakeFileInfo{name: "file2", size: 43}, transfer.Update, new(uint64), transfer.InProgress)
		p.FileHook(&fakeFileInfo{name: "file3", size: 0}, transfer.Update, new(uint64), transfer.InProgress)

		mp := &fakeBarContainer{Progress: mpb.NewWithContext(ctx, mpb.WithOutput(nil))}
		inProgBars := map[string]*mpb.Bar{}
		got := p.updateInProgBars(inProgBars, mp, 2)

		if got, want := len(mp.bars), 2; got != want {
			t.Errorf("BarCount: got %v, want %v", got, want)
		}
		if want := map[string]*mpb.Bar{"file2": mp.bars[0], "file1": mp.bars[1]}; !reflect.DeepEqual(got, want) {
			t.Errorf("updateInProgBars: got %+v, want %+v", got, want)
		}

		cancel()
		mp.Wait()
	})

	t.Run("rankUploaded", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		var p terminalProgress

		v1 := uint64(42)
		p.FileHook(&fakeFileInfo{name: "file1", size: 1000}, transfer.Create, &v1, transfer.InProgress)
		v2 := uint64(43)
		p.FileHook(&fakeFileInfo{name: "file2", size: 1000}, transfer.Update, &v2, transfer.InProgress)
		p.FileHook(&fakeFileInfo{name: "file3", size: 0}, transfer.Update, new(uint64), transfer.InProgress)

		mp := &fakeBarContainer{Progress: mpb.NewWithContext(ctx, mpb.WithOutput(nil))}
		inProgBars := map[string]*mpb.Bar{}
		got := p.updateInProgBars(inProgBars, mp, 2)

		if got, want := len(mp.bars), 2; got != want {
			t.Errorf("BarCount: got %v, want %v", got, want)
		}
		if want := map[string]*mpb.Bar{"file1": mp.bars[0], "file2": mp.bars[1]}; !reflect.DeepEqual(got, want) {
			t.Errorf("updateInProgBars: got %+v, want %+v", got, want)
		}

		cancel()
		mp.Wait()
	})
}

type fakeBarContainer struct {
	*mpb.Progress

	bars []*mpb.Bar
}

func (bc *fakeBarContainer) AddBar(total int64, options ...mpb.BarOption) *mpb.Bar {
	bar := bc.Progress.AddBar(total, options...)
	bc.bars = append(bc.bars, bar)
	return bar
}
func (bc *fakeBarContainer) AddSpinner(total int64, options ...mpb.BarOption) *mpb.Bar {
	bar := bc.Progress.AddSpinner(total, options...)
	bc.bars = append(bc.bars, bar)
	return bar
}

type fakeUpload struct {
	nstatsCalls int
}

func (u *fakeUpload) Stats() transfer.UploadStats {
	u.nstatsCalls++
	return transfer.UploadStats{
		ProcessStats: transfer.ProcessStats{
			InProgress:  1,
			SourceFiles: 2,
		},

		UploadedBytes: 3,
	}
}

type fakeFileInfo struct {
	os.FileInfo
	name string
	size int64
}

func (*fakeFileInfo) Mode() os.FileMode  { return 0 }
func (*fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (fi *fakeFileInfo) Size() int64     { return fi.size }
func (fi *fakeFileInfo) Name() string {
	if fi.name != "" {
		return fi.name
	}
	return "file"
}
