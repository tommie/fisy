package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/tommie/fisy/transfer"
	"golang.org/x/crypto/ssh/terminal"
)

func RunProgress(ctx context.Context, u *transfer.Upload) {
	fd := int(os.Stdout.Fd())
	if !terminal.IsTerminal(fd) {
		return
	}

	tw, _, err := terminal.GetSize(fd)
	if err != nil {
		tw = 80
		glog.Warningf("couldn't get terminal size (defaulting to %v): %v", tw, err)
	}
	tw -= 1 // One character margin.

	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	var printed bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Continue
		}

		showStats(u, tw)
		printed = true
	}

	if printed {
		fmt.Println()
	}
}

func showStats(u *transfer.Upload, maxLength int) {
	st := u.Stats()
	s := fmt.Sprintf("\033[2K%5d / %7s / %d / %d: %c %s\033[1G", st.SourceFiles, "+"+storageBytes(st.UploadedBytes), st.InProgress, st.InodeTable, st.LastFileOperation(), st.LastPath())
	if len(s) > maxLength {
		s = s[:maxLength]
	}
	fmt.Print(s)
}

var storageBytesUnits = []string{
	"B", "kiB", "MiB", "GiB", "PiB",
}

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
	return fmt.Sprintf("%.0f EiB", f)
}
