package transfer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"fmt"
	"github.com/tommie/fisy/fs"
	"golang.org/x/sync/errgroup"
)

func TestLinkSetFinishedFileSingle(t *testing.T) {
	tmpf, err := ioutil.TempFile("", "hardlink_test-")
	if err != nil {
		t.Fatalf("TempFile failed: %v", err)
	}
	defer tmpf.Close()
	defer os.Remove(tmpf.Name())

	sfi, err := tmpf.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	ls := newLinkSet()

	got, _ := ls.FinishedFile(fs.Path(sfi.Name()), sfi)
	if want := uint64(0); got != want {
		t.Errorf("FinishedFile: got %v, want %v", got, want)
	}

	if want := 0; ls.Len() != want {
		t.Errorf("Len: got %v, want %v", ls.Len(), want)
	}
}

func TestLinkSetFulfill(t *testing.T) {
	tmpd, err := ioutil.TempDir("", "hardlink_test-")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(tmpd)

	srcf, err := os.Create(filepath.Join(tmpd, "src"))
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer srcf.Close()

	if err := os.Link(srcf.Name(), filepath.Join(tmpd, "link")); err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	ls := newLinkSet()

	// First file; should have to transfer.
	sfi, err := srcf.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	gotInode, gotPath := ls.FinishedFile("src", sfi)
	if gotInode == 0 {
		t.Fatalf("FinishedFile failed")
	}
	if gotPath != "" {
		t.Fatalf("FinishedFile: got %q, want empty", gotPath)
	}

	if want := 1; ls.Len() != want {
		t.Fatalf("Len: got %v, want %v", ls.Len(), want)
	}

	// First file done.
	ls.Fulfill(gotInode)

	if want := 1; ls.Len() != want {
		t.Fatalf("Len: got %v, want %v", ls.Len(), want)
	}

	// Second file; should link to first file.
	lfi, err := os.Stat(filepath.Join(tmpd, "link"))
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	gotInode2, gotPath2 := ls.FinishedFile("link", lfi)
	if gotInode2 == 0 {
		t.Fatalf("FinishedFile failed")
	}
	if string(gotPath2) != filepath.Base(srcf.Name()) {
		t.Fatalf("FinishedFile: got %q, want %q", gotPath2, filepath.Base(srcf.Name()))
	}

	if want := 0; ls.Len() != want {
		t.Fatalf("Len: got %v, want %v", ls.Len(), want)
	}
}

func TestLinkSetDiscard(t *testing.T) {
	tmpd, err := ioutil.TempDir("", "hardlink_test-")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(tmpd)

	srcf, err := os.Create(filepath.Join(tmpd, "src"))
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer srcf.Close()

	if err := os.Link(srcf.Name(), filepath.Join(tmpd, "link")); err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	ls := newLinkSet()

	// First file; should have to transfer.
	sfi, err := srcf.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	gotInode, gotPath := ls.FinishedFile("src", sfi)
	if gotInode == 0 {
		t.Fatalf("FinishedFile failed")
	}
	if gotPath != "" {
		t.Fatalf("FinishedFile: got %q, want empty", gotPath)
	}

	if want := 1; ls.Len() != want {
		t.Fatalf("Len: got %v, want %v", ls.Len(), want)
	}

	// First file failed.
	ls.Discard(gotInode, fs.Path(sfi.Name()))

	if want := 1; ls.Len() != want {
		t.Fatalf("Len: got %v, want %v", ls.Len(), want)
	}

	// Second file; should have to transfer.
	lfi, err := os.Stat(filepath.Join(tmpd, "link"))
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	gotInode2, gotPath2 := ls.FinishedFile("link", lfi)
	if gotInode2 == 0 {
		t.Fatalf("FinishedFile failed")
	}
	if gotPath2 != "" {
		t.Fatalf("FinishedFile: got %q, want empty", gotPath2)
	}

	// Second file done.
	ls.Fulfill(gotInode)

	if want := 0; ls.Len() != want {
		t.Fatalf("Len: got %v, want %v", ls.Len(), want)
	}
}

func TestLinkSetFulfillIsThreadSane(t *testing.T) {
	tmpd, err := ioutil.TempDir("", "hardlink_test-")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(tmpd)

	srcf, err := os.Create(filepath.Join(tmpd, "src"))
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer srcf.Close()

	if err := os.Link(srcf.Name(), filepath.Join(tmpd, "link")); err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	ls := newLinkSet()

	var ntransfer, nlink int
	var eg errgroup.Group

	for _, name := range []string{"src", "link"} {
		name := name
		eg.Go(func() error {
			fi, err := os.Stat(filepath.Join(tmpd, name))
			if err != nil {
				return err
			}

			gotInode, gotPath := ls.FinishedFile(fs.Path(name), fi)
			if gotInode == 0 {
				return fmt.Errorf("FinishedFile failed")
			}
			if gotPath == "" {
				ntransfer++
				ls.Fulfill(gotInode)
			} else {
				nlink++
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatalf("Failure: %v", err)
	}

	if want := 1; ntransfer != want {
		t.Errorf("ntransfer: got %v, want %v", ntransfer, want)
	}
	if want := 1; nlink != want {
		t.Errorf("nlink: got %v, want %v", nlink, want)
	}
}

func TestLinkSetDiscardIsThreadSane(t *testing.T) {
	tmpd, err := ioutil.TempDir("", "hardlink_test-")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(tmpd)

	srcf, err := os.Create(filepath.Join(tmpd, "src"))
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer srcf.Close()

	if err := os.Link(srcf.Name(), filepath.Join(tmpd, "link")); err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	ls := newLinkSet()

	var ntransfer, nlink int
	var eg errgroup.Group

	for _, name := range []string{"src", "link"} {
		name := name
		eg.Go(func() error {
			fi, err := os.Stat(filepath.Join(tmpd, name))
			if err != nil {
				return err
			}

			gotInode, gotPath := ls.FinishedFile(fs.Path(name), fi)
			if gotInode == 0 {
				return fmt.Errorf("FinishedFile failed")
			}
			if gotPath == "" {
				ntransfer++
				ls.Discard(gotInode, fs.Path(fi.Name()))
			} else {
				nlink++
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatalf("Failure: %v", err)
	}

	if want := 2; ntransfer != want {
		t.Errorf("ntransfer: got %v, want %v", ntransfer, want)
	}
	if want := 0; nlink != want {
		t.Errorf("nlink: got %v, want %v", nlink, want)
	}
}
