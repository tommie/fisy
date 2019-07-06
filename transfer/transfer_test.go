package transfer

import (
	"testing"
	"os"
	"time"
	"fmt"
)

func TestFilePair(t *testing.T) {
	t.Run("fileOperationCreate", func(t *testing.T) {
		fp := &filePair{src: &fakeListingFileInfo{}}

		if got, want := fp.FileOperation(), Create; got != want {
			t.Errorf("FileOperation(%+v): got %c, want %c", fp, got, want)
		}
	})

	t.Run("fileOperationKeepDirectory", func(t *testing.T) {
		fp := &filePair{src: &fakeListingFileInfo{mode: os.ModeDir}, dest: &fakeListingFileInfo{mode: os.ModeDir}}

		if got, want := fp.FileOperation(), Keep; got != want {
			t.Errorf("FileOperation(%+v): got %c, want %c", fp, got, want)
		}
	})

	t.Run("fileOperationUpdateDirectory", func(t *testing.T) {
		fp := &filePair{src: &fakeListingFileInfo{mode: os.ModeDir | 1}, dest: &fakeListingFileInfo{mode: os.ModeDir}}

		if got, want := fp.FileOperation(), Update; got != want {
			t.Errorf("FileOperation(%+v): got %c, want %c", fp, got, want)
		}
	})

	t.Run("fileOperationKeepFile", func(t *testing.T) {
		fp := &filePair{src: &fakeListingFileInfo{}, dest: &fakeListingFileInfo{}}

		if got, want := fp.FileOperation(), Keep; got != want {
			t.Errorf("FileOperation(%+v): got %c, want %c", fp, got, want)
		}
	})

	t.Run("fileOperationUpdateFile", func(t *testing.T) {
		fp := &filePair{src: &fakeListingFileInfo{size: 1}, dest: &fakeListingFileInfo{}}

		if got, want := fp.FileOperation(), Update; got != want {
			t.Errorf("FileOperation(%+v): got %c, want %c", fp, got, want)
		}
	})

	t.Run("fileOperationRemove", func(t *testing.T) {
		fp := &filePair{dest: &fakeListingFileInfo{}}

		if got, want := fp.FileOperation(), Remove; got != want {
			t.Errorf("FileOperation(%+v): got %c, want %c", fp, got, want)
		}
	})
}

func TestUploadFileNeedsTransfer(t *testing.T) {
	now := time.Now()

	tsts := []struct {
		Dest, Src os.FileInfo
		Want      bool
	}{
		{&fakeListingFileInfo{}, &fakeListingFileInfo{}, false},
		{&fakeListingFileInfo{mtime: now.Add(500 * time.Millisecond)}, &fakeListingFileInfo{mtime: now}, false},

		{nil, &fakeListingFileInfo{}, true},
		{&fakeListingFileInfo{size: 42}, &fakeListingFileInfo{size: 4711}, true},
		{&fakeListingFileInfo{mode: 42}, &fakeListingFileInfo{mode: 4711}, true},
		{&fakeListingFileInfo{mtime: now.Add(1500 * time.Millisecond)}, &fakeListingFileInfo{mtime: now}, true},
		{&fakeListingFileInfo{mtime: now}, &fakeListingFileInfo{mtime: now.Add(1500 * time.Millisecond)}, true},
	}
	for _, tst := range tsts {
		t.Run(fmt.Sprint(tst.Src, "/", tst.Dest), func(t *testing.T) {
			if fileNeedsTransfer(tst.Dest, tst.Src) != tst.Want {
				t.Errorf("got %v, want %v", !tst.Want, tst.Want)
			}
		})
	}
}

func TestUploadDirectoryNeedsTransfer(t *testing.T) {
	tsts := []struct {
		Dest, Src os.FileInfo
		Want      bool
	}{
		{&fakeListingFileInfo{}, &fakeListingFileInfo{}, false},

		{nil, &fakeListingFileInfo{}, true},
		{&fakeListingFileInfo{mode: 42}, &fakeListingFileInfo{mode: 4711}, true},
	}
	for _, tst := range tsts {
		t.Run(fmt.Sprint(tst.Src, "/", tst.Dest), func(t *testing.T) {
			if directoryNeedsTransfer(tst.Dest, tst.Src) != tst.Want {
				t.Errorf("got %v, want %v", !tst.Want, tst.Want)
			}
		})
	}
}
