package transfer

import (
	"context"
	"os"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fmt"
	"github.com/pkg/sftp"
	"github.com/tommie/fisy/fs"
)

func TestNewUpload(t *testing.T) {
	const ignoredPath fs.Path = "/dir2/"

	u := newTestUpload(WithIgnoreFilter(func(path fs.Path) bool {
		return path == ignoredPath
	}))

	if got, want := u.process.ignoreFilter(ignoredPath), true; got != want {
		t.Errorf("NewUpload ignoreFilter(%q): got %v, want %v", ignoredPath, got, want)
	}
	if want := 2; u.process.nconc != want {
		t.Errorf("NewUpload nconc: got %v, want %v", u.process.nconc, want)
	}
}

func TestUploadTransfer(t *testing.T) {
	ctx := context.Background()

	t.Run("dir", func(t *testing.T) {
		u := newTestUpload()

		if err := u.transfer(ctx, &filePair{path: "dir1", src: &fakeListingFileInfo{name: "dir1", mode: os.ModeDir}}); err != nil {
			t.Fatalf("transfer failed: %v", err)
		}

		if got, want := u.stats.LastPath(), "dir1"; got != want {
			t.Errorf("stats.LastPath: got %q, want %q", got, want)
		}

		if got, want := int(u.stats.CreatedDirectories), 1; got != want {
			t.Errorf("stats.CreatedDirectories: got %v, want %v", got, want)
		}
	})

	t.Run("file", func(t *testing.T) {
		u := newTestUpload()

		if err := u.transfer(ctx, &filePair{path: "file1", src: &fakeListingFileInfo{name: "file1"}}); err != nil {
			t.Fatalf("transfer failed: %v", err)
		}

		if got, want := u.stats.LastPath(), "file1"; got != want {
			t.Errorf("stats.LastPath: got %q, want %q", got, want)
		}

		if got, want := int(u.stats.UploadedFiles), 1; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
	})

	t.Run("retries", func(t *testing.T) {
		u := newTestUpload()

		if err := u.transfer(ctx, &filePair{path: "retry-file", src: &fakeListingFileInfo{name: "retry-file"}}); err != nil {
			t.Fatalf("transfer failed: %v", err)
		}

		if got, want := int(u.stats.TransferRetries), 1; got != want {
			t.Errorf("stats.TransferRetries: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedFiles), 1; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
	})

	t.Run("fileHook", func(t *testing.T) {
		var fis []os.FileInfo
		var ops []FileOperation
		var errs []error
		u := newTestUpload(WithFileHook(func(fi os.FileInfo, op FileOperation, err error) {
			fis = append(fis, fi)
			ops = append(ops, op)
			errs = append(errs, err)
		}))

		src := &fakeListingFileInfo{name: "file1"}
		if err := u.transfer(ctx, &filePair{path: "file1", src: src}); err != nil {
			t.Fatalf("transfer failed: %v", err)
		}

		if want := []os.FileInfo{&filePairInfo{src, "file1"}, &filePairInfo{src, "file1"}}; !reflect.DeepEqual(fis, want) {
			t.Errorf("transfer FileInfo: got %+v, want %+v", fis, want)
		}
		if want := []FileOperation{Create, Create}; !reflect.DeepEqual(ops, want) {
			t.Errorf("transfer FileOperation: got %+v, want %+v", ops, want)
		}
		if want := []error{InProgress, nil}; !reflect.DeepEqual(errs, want) {
			t.Errorf("transfer error: got %+v, want %+v", errs, want)
		}
	})
}

func TestUploadTransferFile(t *testing.T) {
	t.Run("remove", func(t *testing.T) {
		u := newTestUpload()

		if err := u.transferFile(&filePair{path: "file1", dest: &fakeListingFileInfo{name: "file1"}}); err != nil {
			t.Fatalf("transferFile failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"file1"}; !reflect.DeepEqual(wfs.removeCalls, want) {
			t.Errorf("transferFile: got %v, want %v", wfs.removeCalls, want)
		}

		if got, want := int(u.stats.RemovedDirectories), 0; got != want {
			t.Errorf("stats.RemovedDirectories: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.RemovedFiles), 1; got != want {
			t.Errorf("stats.RemovedFiles: got %v, want %v", got, want)
		}
	})

	t.Run("discarded", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferFile(&filePair{
			path: "failing-symlink",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "failing-symlink", mode: os.ModeSymlink | 1}, inode: 42},
			dest: &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "failing-symlink", mode: os.ModeSymlink}},
		})
		if want := errMocked; err != want {
			t.Fatalf("transferFile error: got %v, want %v", err, want)
		}

		if want := false; u.srcLinks.inodes[42].uploaded != want {
			t.Errorf("srcLinks: got %+v, want %v", u.srcLinks.inodes[42], want)
		}
	})

	t.Run("link", func(t *testing.T) {
		u := newTestUpload()

		u.srcLinks.FinishedFile(fs.Path("firstfile"), &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "firstfile"}, inode: 42})
		u.srcLinks.Fulfill(42)

		if err := u.transferFile(&filePair{path: "file1", src: &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "file1"}, inode: 42}}); err != nil {
			t.Fatalf("transferFile failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := [][]fs.Path{{"firstfile", "file1"}}; !reflect.DeepEqual(wfs.linkCalls, want) {
			t.Errorf("transferFile linkCalls: got %v, want %v", wfs.linkCalls, want)
		}

		if got, want := int(u.stats.UploadedFiles), 1; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedBytes), 0; got != want {
			t.Errorf("stats.UploadedBytes: got %v, want %v", got, want)
		}
	})

	t.Run("linkFirst", func(t *testing.T) {
		u := newTestUpload()

		if err := u.transferFile(&filePair{path: "file1", src: &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "file1", size: 4711}, inode: 42}}); err != nil {
			t.Fatalf("transferFile failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := 0; len(wfs.linkCalls) != want {
			t.Errorf("transferFile linkCalls: got %v, want %v", len(wfs.linkCalls), want)
		}

		if got, want := int(u.srcLinks.inodes[42].nlink), 1; got != want {
			t.Errorf("srcLinks.inodes.nlink: got %v, want %v", got, want)
		}

		if got, want := int(u.stats.UploadedFiles), 1; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedBytes), 4711; got != want {
			t.Errorf("stats.UploadedBytes: got %v, want %v", got, want)
		}
	})

	t.Run("keep", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferFile(&filePair{
			path: "file1",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "file1", size: 4711}},
			dest: &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "file1", size: 4711}},
		})
		if err != nil {
			t.Fatalf("transferFile failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"file1"}; !reflect.DeepEqual(wfs.keepCalls, want) {
			t.Errorf("transferFile keepCalls: got %v, want %v", wfs.keepCalls, want)
		}

		if got, want := int(u.stats.KeptFiles), 1; got != want {
			t.Errorf("stats.KeptFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.KeptBytes), 4711; got != want {
			t.Errorf("stats.KeptBytes: got %v, want %v", got, want)
		}
	})

	t.Run("keepFallsBack", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferFile(&filePair{
			path: "keep-failing-file",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "keep-failing-file"}},
			dest: &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "keep-failing-file"}},
		})
		if err != nil {
			t.Fatalf("transferFile failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"keep-failing-file"}; !reflect.DeepEqual(wfs.keepCalls, want) {
			t.Errorf("transferFile keepCalls: got %v, want %v", wfs.keepCalls, want)
		}
		if want := []fs.Path{"keep-failing-file"}; !reflect.DeepEqual(wfs.createCalls, want) {
			t.Errorf("transferFile createCalls: got %v, want %v", wfs.createCalls, want)
		}

		if got, want := int(u.stats.KeptFiles), 0; got != want {
			t.Errorf("stats.KeptFiles: got %v, want %v", got, want)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferFile(&filePair{
			path: "file1",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "file1", mode: os.ModeSymlink}},
		})
		if err != nil {
			t.Fatalf("transferFile failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := [][]fs.Path{{"symlink-target", "file1"}}; !reflect.DeepEqual(wfs.symlinkCalls, want) {
			t.Errorf("transferFile symlinkCalls: got %v, want %v", wfs.symlinkCalls, want)
		}

		if got, want := int(u.stats.UploadedFiles), 1; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedBytes), len("symlink-target"); got != want {
			t.Errorf("stats.UploadedBytes: got %v, want %v", got, want)
		}
	})

	t.Run("copy", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferFile(&filePair{
			path: "file1",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "file1", size: 4711}},
		})
		if err != nil {
			t.Fatalf("transferFile failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"file1"}; !reflect.DeepEqual(wfs.createCalls, want) {
			t.Errorf("transferFile createCalls: got %v, want %v", wfs.createCalls, want)
		}

		if got, want := int(u.stats.UploadedFiles), 1; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedBytes), 4711; got != want {
			t.Errorf("stats.UploadedBytes: got %v, want %v", got, want)
		}
	})
}

func TestUploadCreateSymlink(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		u := newTestUpload()

		err := u.createSymlink(&filePair{
			path: "file1",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "file1", mode: os.ModeSymlink}},
		})
		if err != nil {
			t.Fatalf("createSymlink failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := [][]fs.Path{{"symlink-target", "file1"}}; !reflect.DeepEqual(wfs.symlinkCalls, want) {
			t.Errorf("symlinkCalls: got %v, want %v", wfs.symlinkCalls, want)
		}

		if got, want := int(u.stats.UploadedFiles), 1; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedBytes), len("symlink-target"); got != want {
			t.Errorf("stats.UploadedBytes: got %v, want %v", got, want)
		}
	})

	t.Run("discarded", func(t *testing.T) {
		u := newTestUpload()

		src := &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "missing-symlink", mode: os.ModeSymlink}, inode: 42}
		u.srcLinks.FinishedFile("missing-symlink", src)
		err := u.createSymlink(&filePair{
			path: "missing-symlink",
			src:  src,
		})
		if want := errDiscarded; err != want {
			t.Fatalf("createSymlink error: got %v, want %v", err, want)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := [][]fs.Path(nil); !reflect.DeepEqual(wfs.symlinkCalls, want) {
			t.Errorf("symlinkCalls: got %v, want %v", wfs.symlinkCalls, want)
		}

		if got, want := int(u.stats.DiscardedFiles), 1; got != want {
			t.Errorf("stats.DiscardedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedFiles), 0; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedBytes), 0; got != want {
			t.Errorf("stats.UploadedBytes: got %v, want %v", got, want)
		}
	})

	t.Run("failed", func(t *testing.T) {
		u := newTestUpload()

		src := &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "failing-symlink", mode: os.ModeSymlink}, inode: 42}
		u.srcLinks.FinishedFile("failing-symlink", src)
		err := u.createSymlink(&filePair{
			path: "failing-symlink",
			src:  src,
		})
		if err != errMocked {
			t.Fatalf("createSymlink error: got %v, want %v", err, errMocked)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := [][]fs.Path(nil); !reflect.DeepEqual(wfs.symlinkCalls, want) {
			t.Errorf("symlinkCalls: got %v, want %v", wfs.symlinkCalls, want)
		}

		if got, want := int(u.stats.DiscardedFiles), 0; got != want {
			t.Errorf("stats.DiscardedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedFiles), 0; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedBytes), 0; got != want {
			t.Errorf("stats.UploadedBytes: got %v, want %v", got, want)
		}
	})
}

func TestUploadCopyFile(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		u := newTestUpload()

		err := u.copyFile(&filePair{
			path: "file1",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "file1", size: 4711}},
		})
		if err != nil {
			t.Fatalf("copyFile failed: %v", err)
		}

		rfs := u.src.(*fakeWriteableFileSystem)
		if want := []fs.Path{"file1"}; !reflect.DeepEqual(rfs.openCalls, want) {
			t.Errorf("openCalls: got %v, want %v", rfs.openCalls, want)
		}
		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"file1"}; !reflect.DeepEqual(wfs.createCalls, want) {
			t.Errorf("createCalls: got %v, want %v", wfs.createCalls, want)
		}
		if want := []fs.Path{"file1"}; !reflect.DeepEqual(wfs.chtimesCalls, want) {
			t.Errorf("chtimesCalls: got %v, want %v", wfs.chtimesCalls, want)
		}

		if got, want := int(u.stats.UploadedFiles), 1; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UploadedBytes), 4711; got != want {
			t.Errorf("stats.UploadedBytes: got %v, want %v", got, want)
		}
	})

	t.Run("discarded", func(t *testing.T) {
		u := newTestUpload()

		src := &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "missing-file"}}
		err := u.copyFile(&filePair{path: "missing-file", src: src})
		if want := errDiscarded; err != want {
			t.Fatalf("copyFile error: got %v, want %v", err, want)
		}

		if got, want := int(u.stats.UploadedFiles), 0; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
	})

	t.Run("create-failed", func(t *testing.T) {
		u := newTestUpload()

		src := &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "create-failing-file"}}
		err := u.copyFile(&filePair{path: "create-failing-file", src: src})
		if want := errMocked; err != want {
			t.Fatalf("copyFile error: got %v, want %v", err, want)
		}

		if got, want := int(u.stats.UploadedFiles), 0; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
	})

	t.Run("create-readonly", func(t *testing.T) {
		u := newTestUpload()

		src := &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "create-readonly-file"}}
		err := u.copyFile(&filePair{path: "create-readonly-file", src: src})
		if err != nil {
			t.Fatalf("copyFile failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"create-readonly-file", "create-readonly-file"}; !reflect.DeepEqual(wfs.createCalls, want) {
			t.Errorf("createCalls: got %v, want %v", wfs.createCalls, want)
		}
		if want := []fs.Path{"create-readonly-file"}; !reflect.DeepEqual(wfs.chtimesCalls, want) {
			t.Errorf("chtimesCalls: got %v, want %v", wfs.chtimesCalls, want)
		}
		if got, want := int(u.stats.UploadedFiles), 1; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
	})

	t.Run("chmod-failed", func(t *testing.T) {
		u := newTestUpload()

		src := &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "chmod-failing-file"}}
		err := u.copyFile(&filePair{path: "chmod-failing-file", src: src})
		if want := errMocked; err != want {
			t.Fatalf("copyFile error: got %v, want %v", err, want)
		}

		if got, want := int(u.stats.UploadedFiles), 0; got != want {
			t.Errorf("stats.UploadedFiles: got %v, want %v", got, want)
		}
	})
}

func TestUploadTransferDirectory(t *testing.T) {
	t.Run("remove", func(t *testing.T) {
		u := newTestUpload()

		if err := u.transferDirectory(&filePair{path: "dir1", dest: &fakeListingFileInfo{name: "dir1", mode: os.ModeDir}}); err != nil {
			t.Fatalf("transferDirectory failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"dir1"}; !reflect.DeepEqual(wfs.removeAllCalls, want) {
			t.Errorf("transferDirectory: got %v, want %v", wfs.removeAllCalls, want)
		}

		if got, want := int(u.stats.RemovedDirectories), 1; got != want {
			t.Errorf("stats.RemovedDirectories: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.RemovedFiles), 0; got != want {
			t.Errorf("stats.RemovedFiles: got %v, want %v", got, want)
		}
	})

	t.Run("keep", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferDirectory(&filePair{
			path: "dir1",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "dir1", mode: os.ModeDir}},
			dest: &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "dir1", mode: os.ModeDir}},
		})
		if err != nil {
			t.Fatalf("transferDirectory failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"dir1"}; !reflect.DeepEqual(wfs.keepCalls, want) {
			t.Errorf("keepCalls: got %v, want %v", wfs.keepCalls, want)
		}

		if got, want := int(u.stats.KeptDirectories), 1; got != want {
			t.Errorf("stats.KeptDirectories: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.KeptFiles), 0; got != want {
			t.Errorf("stats.KeptFiles: got %v, want %v", got, want)
		}
	})

	t.Run("keepFallsBack", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferDirectory(&filePair{
			path: "keep-failing-dir",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "keep-failing-dir", mode: os.ModeDir}},
			dest: &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "keep-failing-dir", mode: os.ModeDir}},
		})
		if err != nil {
			t.Fatalf("transferDirectory failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"keep-failing-dir"}; !reflect.DeepEqual(wfs.keepCalls, want) {
			t.Errorf("keepCalls: got %v, want %v", wfs.keepCalls, want)
		}
		if want := []fs.Path{"keep-failing-dir"}; !reflect.DeepEqual(wfs.mkdirCalls, want) {
			t.Errorf("mkdirCalls: got %v, want %v", wfs.mkdirCalls, want)
		}
		if want := []fs.Path{"keep-failing-dir"}; !reflect.DeepEqual(wfs.chtimesCalls, want) {
			t.Errorf("chtimesCalls: got %v, want %v", wfs.chtimesCalls, want)
		}

		if got, want := int(u.stats.KeptDirectories), 0; got != want {
			t.Errorf("stats.KeptDirectories: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UpdatedDirectories), 1; got != want {
			t.Errorf("stats.UpdatedDirectories: got %v, want %v", got, want)
		}
	})

	t.Run("create", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferDirectory(&filePair{
			path: "dir1",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "dir1", mode: os.ModeDir}},
		})
		if err != nil {
			t.Fatalf("transferDirectory failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"dir1"}; !reflect.DeepEqual(wfs.mkdirCalls, want) {
			t.Errorf("mkdirCalls: got %v, want %v", wfs.mkdirCalls, want)
		}
		if want := []fs.Path{"dir1"}; !reflect.DeepEqual(wfs.chtimesCalls, want) {
			t.Errorf("chtimesCalls: got %v, want %v", wfs.chtimesCalls, want)
		}

		if got, want := int(u.stats.CreatedDirectories), 1; got != want {
			t.Errorf("stats.CreatedDirectories: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UpdatedDirectories), 0; got != want {
			t.Errorf("stats.UpdatedDirectories: got %v, want %v", got, want)
		}
	})

	t.Run("update", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferDirectory(&filePair{
			path: "dir1",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "dir1", mode: os.ModeDir}},
			dest: &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "dir1", mode: 0123 | os.ModeDir}},
		})
		if err != nil {
			t.Fatalf("transferDirectory failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"dir1"}; !reflect.DeepEqual(wfs.mkdirCalls, want) {
			t.Errorf("mkdirCalls: got %v, want %v", wfs.mkdirCalls, want)
		}
		if want := []fs.Path(nil); !reflect.DeepEqual(wfs.lchownCalls, want) {
			t.Errorf("lchownCalls: got %v, want %v", wfs.lchownCalls, want)
		}
		if want := []fs.Path(nil); !reflect.DeepEqual(wfs.chmodCalls, want) {
			t.Errorf("chmodCalls: got %v, want %v", wfs.chmodCalls, want)
		}
		if want := []fs.Path{"dir1"}; !reflect.DeepEqual(wfs.chtimesCalls, want) {
			t.Errorf("chtimesCalls: got %v, want %v", wfs.chtimesCalls, want)
		}

		if got, want := int(u.stats.CreatedDirectories), 0; got != want {
			t.Errorf("stats.CreatedDirectories: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UpdatedDirectories), 1; got != want {
			t.Errorf("stats.UpdatedDirectories: got %v, want %v", got, want)
		}
	})

	t.Run("update-existing-dir", func(t *testing.T) {
		u := newTestUpload()

		err := u.transferDirectory(&filePair{
			path: "update-existing-dir",
			src:  &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "update-existing-dir", mode: os.ModeDir}},
			dest: &fakeUploadFileInfo{fakeListingFileInfo: fakeListingFileInfo{name: "update-existing-dir", mode: 0123 | os.ModeDir}},
		})
		if err != nil {
			t.Fatalf("transferDirectory failed: %v", err)
		}

		wfs := u.dest.(*fakeWriteableFileSystem)
		if want := []fs.Path{"update-existing-dir"}; !reflect.DeepEqual(wfs.mkdirCalls, want) {
			t.Errorf("mkdirCalls: got %v, want %v", wfs.mkdirCalls, want)
		}
		if want := []fs.Path{"update-existing-dir"}; !reflect.DeepEqual(wfs.lchownCalls, want) {
			t.Errorf("lchownCalls: got %v, want %v", wfs.lchownCalls, want)
		}
		if want := []fs.Path{"update-existing-dir"}; !reflect.DeepEqual(wfs.chmodCalls, want) {
			t.Errorf("chmodCalls: got %v, want %v", wfs.chmodCalls, want)
		}
		if want := []fs.Path{"update-existing-dir"}; !reflect.DeepEqual(wfs.chtimesCalls, want) {
			t.Errorf("chtimesCalls: got %v, want %v", wfs.chtimesCalls, want)
		}

		if got, want := int(u.stats.CreatedDirectories), 0; got != want {
			t.Errorf("stats.CreatedDirectories: got %v, want %v", got, want)
		}
		if got, want := int(u.stats.UpdatedDirectories), 1; got != want {
			t.Errorf("stats.UpdatedDirectories: got %v, want %v", got, want)
		}
	})
}

func TestUploadStats(t *testing.T) {
	t.Run("Stats", func(t *testing.T) {
		u := newTestUpload()
		want := UploadStats{
			ProcessStats: ProcessStats{
				InProgress: 13,
			},
			InodeTable:         1,
			UploadedBytes:      2,
			UploadedFiles:      3,
			CreatedDirectories: 4,
			UpdatedDirectories: 5,
			KeptBytes:          6,
			KeptFiles:          7,
			KeptDirectories:    8,
			RemovedFiles:       9,
			RemovedDirectories: 10,
			DiscardedFiles:     11,
			TransferRetries:    12,

			lastPair: &atomic.Value{},
		}
		want.lastPair.Store(&filePair{})

		u.srcLinks.inodes[42] = nil
		u.stats = want
		got := u.Stats()

		if !reflect.DeepEqual(got, want) {
			t.Errorf("CopyFrom: got %+v, want %+v", got, want)
		}
	})

	t.Run("lastPath", func(t *testing.T) {
		us := UploadStats{
			lastPair: &atomic.Value{},
		}
		us.lastPair.Store(&filePair{path: "file"})

		if want := "file"; us.LastPath() != want {
			t.Errorf("LastPath: got %v, want %v", us.LastPath(), want)
		}
	})

	t.Run("lastFileOperationCreate", func(t *testing.T) {
		us := UploadStats{
			lastPair: &atomic.Value{},
		}
		us.lastPair.Store(&filePair{src: &fakeListingFileInfo{}})

		if want := Create; us.LastFileOperation() != want {
			t.Errorf("LastFileOperation: got %v, want %v", us.LastPath(), want)
		}
	})
}

func newTestUpload(opts ...UploadOpt) *Upload {
	opts = append(opts, WithConcurrency(2))
	return NewUpload(
		&fakeWriteableFileSystem{
			fakeListingFileSystem: fakeListingFileSystem{
				fis: map[fs.Path][]os.FileInfo{
					".": []os.FileInfo{
						&fakeListingFileInfo{name: "file1", mode: 0},
						&fakeListingFileInfo{name: "dir2", mode: os.ModeDir},
						&fakeListingFileInfo{name: "dir1", mode: os.ModeDir},
					},
					"dir1": []os.FileInfo{
						&fakeListingFileInfo{name: "file2", mode: 0},
					},
					"dir2": []os.FileInfo{
						&fakeListingFileInfo{name: "file-removed", mode: 0},
						&fakeListingFileInfo{name: "removed-file", mode: 0},
					},
				},
				data: map[fs.Path][]byte{
					"file1":        make([]byte, 4711),
					"file2":        make([]byte, 4711),
					"file-removed": make([]byte, 4711),
				},
			},
		},
		&fakeWriteableFileSystem{
			fakeListingFileSystem: fakeListingFileSystem{
				fis: map[fs.Path][]os.FileInfo{
					".": []os.FileInfo{
						&fakeListingFileInfo{name: "file1", mode: 0},
						&fakeListingFileInfo{name: "dir1", mode: os.ModeDir},
						&fakeListingFileInfo{name: "dir2", mode: os.ModeDir},
					},
					"dir1": []os.FileInfo{
						&fakeListingFileInfo{name: "file-new", mode: 0},
						&fakeListingFileInfo{name: "new-file", mode: 0},
						&fakeListingFileInfo{name: "file2", mode: 0},
					},
					"dir2": []os.FileInfo{
						&fakeListingFileInfo{name: "file3", mode: 0},
					},
				},
				data: map[fs.Path][]byte{
					"file1":    make([]byte, 4711),
					"file-new": make([]byte, 4711),
					"new-file": make([]byte, 4711),
					"file3":    make([]byte, 4711),
				},
			},
		}, opts...)
}

var errMocked = fmt.Errorf("mocked")

type fakeWriteableFileSystem struct {
	fakeListingFileSystem

	failedCreate   bool
	openCalls      []fs.Path
	chtimesCalls   []fs.Path
	chmodCalls     []fs.Path
	lchownCalls    []fs.Path
	createCalls    []fs.Path
	keepCalls      []fs.Path
	mkdirCalls     []fs.Path
	linkCalls      [][]fs.Path
	symlinkCalls   [][]fs.Path
	removeCalls    []fs.Path
	removeAllCalls []fs.Path
}

func (wfs *fakeWriteableFileSystem) Open(path fs.Path) (fs.FileReader, error) {
	wfs.openCalls = append(wfs.openCalls, path)
	if path == "missing-file" {
		return nil, os.ErrNotExist
	}

	return wfs.fakeListingFileSystem.Open(path)
}

func (wfs *fakeWriteableFileSystem) Readlink(path fs.Path) (fs.Path, error) {
	if path == "missing-symlink" {
		return "", os.ErrNotExist
	}
	if path == "failing-symlink" {
		return "", errMocked
	}
	return "symlink-target", nil
}

func (wfs *fakeWriteableFileSystem) Create(path fs.Path) (fs.FileWriter, error) {
	wfs.createCalls = append(wfs.createCalls, path)
	if path == "retry-file" && !wfs.failedCreate {
		wfs.failedCreate = true
		return nil, sftp.ErrSshFxConnectionLost
	}
	if path == "create-readonly-file" && !wfs.failedCreate {
		wfs.failedCreate = true
		return nil, os.ErrPermission
	}
	if path == "create-failing-file" {
		return nil, errMocked
	}

	return &fakeFileWriter{failChmod: path == "chmod-failing-file"}, nil
}

func (wfs *fakeWriteableFileSystem) Chtimes(path fs.Path, atime, mtime time.Time) error {
	wfs.chtimesCalls = append(wfs.chtimesCalls, path)
	return nil
}

func (wfs *fakeWriteableFileSystem) Chmod(path fs.Path, mode os.FileMode) error {
	wfs.chmodCalls = append(wfs.chmodCalls, path)
	return nil
}

func (wfs *fakeWriteableFileSystem) Lchown(path fs.Path, uid, gid int) error {
	wfs.lchownCalls = append(wfs.lchownCalls, path)
	return nil
}

func (wfs *fakeWriteableFileSystem) Keep(path fs.Path) error {
	wfs.keepCalls = append(wfs.keepCalls, path)
	if path == "keep-failing-file" || path == "keep-failing-dir" {
		return os.ErrPermission
	}
	return nil
}

func (wfs *fakeWriteableFileSystem) Link(src, dest fs.Path) error {
	wfs.linkCalls = append(wfs.linkCalls, []fs.Path{src, dest})
	return nil
}

func (wfs *fakeWriteableFileSystem) Symlink(src, dest fs.Path) error {
	wfs.symlinkCalls = append(wfs.symlinkCalls, []fs.Path{src, dest})
	return nil
}

func (wfs *fakeWriteableFileSystem) Mkdir(path fs.Path, mode os.FileMode, uid, gid int) error {
	wfs.mkdirCalls = append(wfs.mkdirCalls, path)
	if path == "update-existing-dir" {
		return os.ErrExist
	}
	return nil
}

func (wfs *fakeWriteableFileSystem) Remove(path fs.Path) error {
	wfs.removeCalls = append(wfs.removeCalls, path)
	return nil
}

func (wfs *fakeWriteableFileSystem) RemoveAll(path fs.Path) error {
	wfs.removeAllCalls = append(wfs.removeAllCalls, path)
	return nil
}

type fakeFileWriter struct {
	fs.FileWriter

	failChmod bool
	n         int
}

func (*fakeFileWriter) Close() error {
	return nil
}

func (fw *fakeFileWriter) Write(bs []byte) (int, error) {
	fw.n += len(bs)
	return len(bs), nil
}

func (fw *fakeFileWriter) Chmod(os.FileMode) error {
	if fw.failChmod {
		return errMocked
	}
	return nil
}

func (*fakeFileWriter) Chown(uid, gid int) error {
	return nil
}

type fakeUploadFileInfo struct {
	fakeListingFileInfo

	inode uint64
}

func (fi *fakeUploadFileInfo) Sys() interface{} {
	if fi.inode == 0 {
		return nil
	}
	return &syscall.Stat_t{Ino: fi.inode, Nlink: 2}
}
