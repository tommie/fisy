package terminal

import (
	"context"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/cwriter"
	"github.com/vbauerster/mpb/v7/decor"

	"github.com/tommie/fisy/transfer"
)

type Progress interface {
	FileHook(os.FileInfo, transfer.FileOperation, *uint64, error)
	RunUpload(context.Context, Upload)
	FinishUpload(Upload)
}

// A terminalProgress reports progress to a terminal.
type terminalProgress struct {
	w      io.Writer
	period time.Duration

	inProgress sync.Map // map[string]*terminalFile
}

// NewProgress creates a new progress reporter. If the writer is not a
// terminal, progress reporting is disabled.
func NewProgress(w io.Writer, period time.Duration) Progress {
	if f, ok := w.(*os.File); !ok {
		return NoOpProgress{}
	} else if f == nil || !cwriter.IsTerminal(int(f.Fd())) {
		return NoOpProgress{}
	}

	return &terminalProgress{
		w:      w,
		period: period,
	}
}

// FileHook is a transfer.FileHook that lets progress add bars for
// slow files.
func (p *terminalProgress) FileHook(fi os.FileInfo, op transfer.FileOperation, uploadedBytes *uint64, err error) {
	if err == transfer.InProgress {
		glog.Infof("Transfer %v, %d bytes: %s", op, fi.Size(), fi.Name())
		p.inProgress.Store(fi.Name(), &terminalFile{
			fi:            fi,
			op:            op,
			uploadedBytes: uploadedBytes,
		})
		return
	}

	p.inProgress.Delete(fi.Name())
}

// RunUpload displays progress updates until the context is cancelled.
func (p *terminalProgress) RunUpload(ctx context.Context, u Upload) {
	mp := mpb.NewWithContext(
		ctx,
		mpb.WithOutput(p.w),
		mpb.WithRefreshRate(p.period),
		mpb.PopCompletedMode(),
	)

	p.runUpload(ctx, mp, u)

	mp.Wait()
}

func (p *terminalProgress) runUpload(ctx context.Context, mp barContainer, u Upload) {
	bytesSp := mp.AddSpinner(
		0,
		mpb.BarPriority(1),
		mpb.PrependDecorators(
			decor.Name("Uploaded"),
			decor.TotalKibiByte(" % .1f"),
		),
	)
	filesSp := mp.AddSpinner(
		0,
		mpb.BarPriority(1),
		mpb.PrependDecorators(
			decor.Name("Files"),
			decor.TotalNoUnit(" %d, "),
			decor.Elapsed(decor.ET_STYLE_GO),
		),
	)
	inProgBar := mp.AddBar(
		0,
		mpb.BarPriority(1),
		mpb.PrependDecorators(
			decor.Name("In Progress"),
			decor.TotalNoUnit(" %d"),
		),
	)

	inProgBars := map[string]*mpb.Bar{}

	t := time.NewTicker(p.period)
	defer t.Stop()
loop:
	for {
		stats := u.Stats()
		if inProgBar.Current() < int64(stats.InProgress) {
			inProgBar.SetTotal(int64(stats.InProgress), false)
		}
		inProgBar.SetCurrent(int64(stats.InProgress))
		filesSp.SetTotal(int64(stats.SourceFiles), false)
		filesSp.SetCurrent(int64(stats.SourceFiles))
		bytesSp.SetTotal(int64(stats.UploadedBytes), false)
		bytesSp.SetCurrent(int64(stats.UploadedBytes))

		inProgBars = p.updateInProgBars(inProgBars, mp, 10)

		select {
		case <-ctx.Done():
			break loop
		case <-t.C:
			// Continue
		}
	}
}

func (p *terminalProgress) updateInProgBars(inProgBars map[string]*mpb.Bar, mp barContainer, nmax int) map[string]*mpb.Bar {
	// Show the top-10 furthest-ETA where something must be
	// transferred. Assuming equal transfer rate, that means the files
	// with the most remaining to upload.

	// Create a slice of transfer operations.
	tfs := make([]*terminalFile, 0, len(inProgBars))
	p.inProgress.Range(func(_, value interface{}) bool {
		tf := value.(*terminalFile)
		if tf.op != transfer.Create && tf.op != transfer.Update {
			return true
		}
		tfs = append(tfs, tf)
		return true
	})

	// Reverse-sort on ETA. Furthest first.
	sort.Slice(tfs, func(i, j int) bool {
		return tfs[i].fi.Size()-int64(tfs[i].UploadedBytes()) > tfs[j].fi.Size()-int64(tfs[j].UploadedBytes())
	})
	if len(tfs) > nmax {
		tfs = tfs[:nmax]
	}

	newInProgBars := make(map[string]*mpb.Bar, len(tfs))
	for _, tf := range tfs {
		newInProgBars[tf.fi.Name()] = inProgBars[tf.fi.Name()]
	}

	// Clean up the bars we are no longer interested in.
	for path, bar := range inProgBars {
		if _, ok := newInProgBars[path]; !ok {
			bar.Abort(true)
		}
	}

	// Add new bars as needed, and update the current value.
	for _, tf := range tfs {
		path := tf.fi.Name()
		bar := newInProgBars[path]
		if bar == nil {
			bar = mp.AddBar(
				tf.fi.Size(),
				mpb.BarPriority(10),
				mpb.PrependDecorators(decor.Name(shortPath(path, 40), decor.WC{W: 40, C: decor.DSyncWidthR})),
				mpb.AppendDecorators(decor.TotalKibiByte("% .1f")),
			)
			newInProgBars[path] = bar
		}
		bar.SetCurrent(int64(tf.UploadedBytes()))
	}

	return newInProgBars
}

func shortPath(p string, max int) string {
	if len(p) < max {
		return p
	}
	return p[:max/2-2] + "..." + p[max/2+2:]
}

// FinishUpload writes summary statistics at the end of an upload.
func (p *terminalProgress) FinishUpload(u Upload) {
	stats := u.Stats()
	glog.Infof("All done: %+v", stats)
}

type barContainer interface {
	AddBar(total int64, options ...mpb.BarOption) *mpb.Bar
	AddSpinner(total int64, options ...mpb.BarOption) *mpb.Bar
}

type Upload interface {
	Stats() transfer.UploadStats
}

type terminalFile struct {
	fi            os.FileInfo
	op            transfer.FileOperation
	uploadedBytes *uint64
}

func (tf *terminalFile) UploadedBytes() uint64 {
	return atomic.LoadUint64(tf.uploadedBytes)
}
