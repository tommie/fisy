package terminal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/tommie/fisy/transfer"
	"golang.org/x/crypto/ssh/terminal"
)

// A Progress reports progress to a terminal.
type Progress struct {
	w      io.Writer
	period time.Duration
	width  int
	start  time.Time

	mu sync.Mutex
}

// NewProgress creates a new progress reporter. If the writer is not a
// terminal, progress reporting is disabled.
func NewProgress(w io.Writer, period time.Duration) *Progress {
	width := 0
	if f, ok := w.(*os.File); ok {
		fd := int(f.Fd())
		if isTerminal(fd) {
			// TODO: React to SIGWINCH.
			tw, _, err := terminalGetSize(fd)
			if err != nil {
				tw = 80
				glog.Warningf("couldn't get terminal size (defaulting to %v): %v", tw, err)
			}
			width = tw - 1 // One character margin.
		}
	}

	return &Progress{
		w:      w,
		period: period,
		width:  width,
		start:  time.Now(),
	}
}

var (
	// terminalGetSize is a mock injection point.
	terminalGetSize = terminal.GetSize
	// isTerminal is a mock injection point.
	isTerminal = terminal.IsTerminal
	// timeNow is a mock injection point.
	timeNow = time.Now
)

type Upload interface {
	Stats() transfer.UploadStats
}

// RunUpload displays progress updates until the context is cancelled.
func (p *Progress) RunUpload(ctx context.Context, u Upload) {
	if p.width == 0 {
		return
	}

	t := time.NewTicker(p.period)
	defer t.Stop()

	var printed bool
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-t.C:
			// Continue
		}

		st := u.Stats()
		s := p.formatUploadStats(&st)
		fmt.Fprint(p.w, "\033[1G", s, "\033[K\033[1G")
		printed = true
	}

	if printed {
		fmt.Fprintln(p.w)
	}
}

// printUploadStats displays ongoing progress for an Upload.
func (p *Progress) formatUploadStats(st *transfer.UploadStats) string {
	s := fmt.Sprintf(
		"%10v / %5d / %7s / %d: %c %s",
		timeNow().Sub(p.start),
		st.SourceFiles,
		"+"+storageBytes(st.UploadedBytes),
		st.InProgress,
		st.LastFileOperation(),
		st.LastPath())
	if len(s) > p.width {
		s = s[:p.width]
	}
	return s
}

// FinishUpload writes summary statistics at the end of an upload.
func (p *Progress) FinishUpload(u Upload) {
	stats := u.Stats()
	glog.Infof("All done in %v: %+v", time.Now().Sub(p.start), stats)
	fmt.Fprintf(
		p.w,
		"All done in %v. Uploaded %v in %v file(s). Kept %v in %v file(s).\n",
		timeNow().Sub(p.start),
		storageBytes(stats.UploadedBytes), stats.UploadedFiles,
		storageBytes(stats.KeptBytes), stats.KeptFiles)
}

// storageBytesUnits is the list of multiples of 1024.
var storageBytesUnits = []string{
	"B", "kiB", "MiB", "GiB", "TiB", "PiB",
}

// storageBytes renders an integer as a human-friendly string.
func storageBytes(v uint64) string {
	f := float64(v)
	for _, unit := range storageBytesUnits {
		if f == 0 {
			return fmt.Sprintf("%.0f %s", f, unit)
		} else if f < 16 {
			return fmt.Sprintf("%.1f %s", f, unit)
		} else if f < 512 {
			return fmt.Sprintf("%.0f %s", f, unit)
		}
		f /= 1024
	}
	return fmt.Sprintf("%.1f EiB", f)
}
