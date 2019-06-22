package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

var (
	cowIsAWriteableFileSystem WriteableFileSystem = &COW{}
	now                                           = time.Date(2019, 2, 1, 15, 4, 5, 0, time.UTC)
)

func TestNewCOWSanityChecks(t *testing.T) {
	t.Run("hostIsEmpty", func(t *testing.T) {
		fs, done := newTestCOW(t)
		defer done()

		_, err := NewCOW(fs.fs, "", now)
		if err != ErrHostIsEmpty {
			t.Fatalf("NewCOW error: got %v, want ErrHostIsEmpty", err)
		}
	})

	t.Run("timestampTooOld", func(t *testing.T) {
		fs, done := newTestCOW(t)
		defer done()

		_, err := NewCOW(fs.fs, "test", now.Add(-2*time.Hour))
		if err == nil || !strings.Contains(err.Error(), "newer timestamp") {
			t.Fatalf("NewCOW error: got %v, want containing %q", err, "newer timestamp")
		}
	})

	t.Run("findsLatestForOtherHost", func(t *testing.T) {
		fs, done := newTestCOW(t)
		defer done()

		fs2, err := NewCOW(fs.fs, "test2", now.Add(1*time.Hour))
		if err != nil {
			t.Fatalf("NewCOW failed: %v", err)
		}

		if want := filepath.Join("test", now.Add(-1*time.Hour).Format("2006-01-02T15-04-05.000000")); string(fs2.rroot) != want {
			t.Errorf("NewCOW: got rroot %q, want %q", fs2.rroot, want)
		}

		if want := filepath.Join("test2", now.Add(1*time.Hour).Format("2006-01-02T15-04-05.000000")); string(fs2.wroot) != want {
			t.Errorf("NewCOW: got rroot %q, want %q", fs2.wroot, want)
		}
	})

	t.Run("canStartFromNothing", func(t *testing.T) {
		tmpd, err := ioutil.TempDir("", "cowfs-")
		if err != nil {
			t.Fatalf("TempDir failed: %v", err)
		}
		defer os.RemoveAll(tmpd)

		_, err = NewCOW(NewLocal(tmpd), "test", now)
		if err != nil {
			t.Fatalf("NewCow failed: %v", err)
		}
	})
}

func TestCOWOpen(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	fr, err := fs.Open(Path("file1"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := fr.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestCOWOpenFailsNoPerms(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	_, err := fs.Open(Path("file-noperms"))
	if !os.IsPermission(err) {
		t.Fatalf("Open error: got %v, want EPERM", err)
	}
}

func TestCOWFileReader(t *testing.T) {
	t.Run("Read", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestCOW(t)
		defer done()

		fr, err := fs.Open(Path("file1"))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer fr.Close()

		got, err := ioutil.ReadAll(fr)
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}

		if want := "content 1\n"; string(got) != want {
			t.Errorf("ReadAll: got %v, want %q", got, want)
		}
	})

	t.Run("Readdir", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestCOW(t)
		defer done()

		fr, err := fs.Open(Path("dir1"))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer fr.Close()

		got, err := fr.Readdir()
		if err != nil {
			t.Fatalf("Readdir failed: %v", err)
		}
		sort.Slice(got, func(i, j int) bool { return got[i].Name() < got[j].Name() })

		de := findMemDirEnt(testTree(), "dir1")
		want := de.Children
		if len(got) != len(want) {
			t.Errorf("Readdir: got len %d, want len %d", len(got), len(want))
		}
		for i, got := range got {
			want := want[i]
			if got.Name() != want.Name {
				t.Errorf("Readdir name: got %+v, want %+v", got, want)
			}
			if got.Mode() != want.Mode {
				t.Errorf("Readdir mode: got %+v, want %+v", got, want)
			}
		}
	})

	t.Run("ReaddirFailsNotDir", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestCOW(t)
		defer done()

		fr, err := fs.Open(Path("file1"))
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer fr.Close()

		_, err = fr.Readdir()
		if !strings.Contains(err.Error(), "directory") {
			t.Fatalf("Readdir error: got %v, want ENOTDIR", err)
		}
	})
}

func TestCOWReadlink(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	got, err := fs.Readlink(Path("symlink1"))
	if err != nil {
		t.Fatalf("Readlink failed: %v", err)
	}

	if want := Path("file1"); got != want {
		t.Errorf("Readlink: got %q, want %q", got, want)
	}
}

func TestCOWStat(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	syscallStatfs = func(path string, sfs *syscall.Statfs_t) error {
		if root := string(fs.fs.(*Local).root); path != root {
			t.Errorf("Statfs path: got %q, want %q", path, root)
		}
		sfs.Frsize = 3
		sfs.Bavail = 5
		return nil
	}
	defer func() {
		syscallStatfs = syscall.Statfs
	}()

	got, err := fs.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if want := 3 * 5; got.FreeSpace != uint64(want) {
		t.Fatalf("Stat FreeSpace: got %v, want %v", got.FreeSpace, want)
	}
}

func TestCOWFinish(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Keep(Path("file1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}

	err = fs.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	root := string(fs.fs.(*Local).root)
	nows := now.Format("2006-01-02T15-04-05.000000")
	if _, err := os.Stat(filepath.Join(root, "test", nows+string(completeSuffix))); err != nil {
		t.Errorf("Stat(%q) failed: %v", filepath.Join("test", nows+string(completeSuffix)), err)
	}
	if _, err := os.Stat(filepath.Join(root, string(latestPath))); err != nil {
		t.Errorf("Stat(%q) failed: %v", latestPath, err)
	}
	if _, err := os.Stat(filepath.Join(root, "test", string(latestPath))); err != nil {
		t.Errorf("Stat(%q) failed: %v", filepath.Join("test", string(latestPath)), err)
	}
}

func TestCOWCreate(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	fw, err := fs.Create(Path("file-create"))
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := fw.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name:    "file-create",
		Mode:    0666 &^ testUmask,
		Content: "",
	})

	if err := checkTestCOW(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestCOWFileWriter(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	fw, err := fs.Create(Path("file-create"))
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer fw.Close()

	_, err = fmt.Fprint(fw, "content create\n")
	if err != nil {
		t.Fatalf("Fprint failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name:    "file-create",
		Mode:    0666 &^ testUmask,
		Content: "content create\n",
	})

	if err := checkTestCOW(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestCOWKeepFile(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Keep(Path("file1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}

	wroot := string(fs.fs.(*Local).root.Resolve(fs.wroot))
	_, err = os.Lstat(filepath.Join(wroot, "file1"))
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
}

func TestCOWKeepDirectory(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Keep(Path("dir1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}

	wroot := string(fs.fs.(*Local).root.Resolve(fs.wroot))
	_, err = os.Lstat(filepath.Join(wroot, "dir1"))
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}
}

func TestCOWMkdir(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Mkdir(Path("dir-make"), 0700, os.Geteuid(), os.Geteuid())
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name: "dir-make",
		Mode: 0700 | os.ModeDir,
	})

	if err := checkTestCOW(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestCOWMkdirCleansUpOnFailure(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Mkdir(Path("dir-make"), 0700, 1234, 1234)
	if err == nil {
		t.Fatal("Mkdir error: got nil, want EPERM")
	}

	if err := checkTestCOW(fs, testTree()); err != nil {
		t.Error(err)
	}
}

func TestCOWLink(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Keep(Path("file1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}
	err = fs.Link(Path("file1"), Path("hardlink-file1"))
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name:    "hardlink-file1",
		Mode:    0666,
		Content: "file1",
	})

	if err := checkTestCOW(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestCOWSymlink(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Symlink(Path("file1"), Path("symlink-create"))
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name:    "symlink-create",
		Mode:    0777 | os.ModeSymlink,
		Content: "file1",
	})

	if err := checkTestCOW(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestCOWRename(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Keep(Path("file1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}
	err = fs.Rename(Path("file1"), Path("file-rename"))
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	tree := testTree()
	findMemDirEnt(tree, "file1").Name = "file-rename"
	findMemDirEnt(tree, "hardlink1").Content = "file-rename"

	if err := checkTestCOW(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestCOWRemoveAll(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestCOW(t)
		defer done()

		err := fs.RemoveAll(Path("hardlink1"))
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "hardlink1")

		if err := checkTestCOW(fs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dir", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestCOW(t)
		defer done()

		err := fs.RemoveAll(Path("dir1"))
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "dir1")

		if err := checkTestCOW(fs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dir-private", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestCOW(t)
		defer done()

		err := fs.RemoveAll(Path("dir-private"))
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "dir-private")

		if err := checkTestCOW(fs, tree); err != nil {
			t.Error(err)
		}
	})
}

func TestCOWRemove(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestCOW(t)
		defer done()

		err := fs.Remove(Path("hardlink1"))
		if err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "hardlink1")

		if err := checkTestCOW(fs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dir", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestCOW(t)
		defer done()

		err := fs.Remove(Path("dir1/dir-empty"))
		if err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "dir1/dir-empty")

		if err := checkTestCOW(fs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dirSucceedsOnNonEmpty", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestCOW(t)
		defer done()

		if err := fs.Remove(Path("dir1")); err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		if err := checkTestCOW(fs, testTree()); err != nil {
			t.Error(err)
		}
	})
}

func TestCOWChmod(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Keep(Path("file1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}
	err = fs.Chmod(Path("file1"), 0600)
	if err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	tree := testTree()
	findMemDirEnt(tree, "file1").Mode = 0600
	findMemDirEnt(tree, "hardlink1").Mode = 0600

	if err := checkTestCOW(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestCOWLchown(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	osLchown = func(path string, uid, gid int) error {
		root := string(fs.fs.(*Local).root.Resolve(fs.wroot))
		if want := filepath.Join(root, "file1"); path != want {
			t.Errorf("Lchown path: got %q, want %q", path, want)
		}
		return nil
	}
	defer func() {
		osLchown = os.Lchown
	}()

	err := fs.Keep(Path("file1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}
	err = fs.Lchown(Path("file1"), 42, 43)
	if err != nil {
		t.Fatalf("Lchown failed: %v", err)
	}

	if err := checkTestCOW(fs, testTree()); err != nil {
		t.Error(err)
	}
}

func TestCOWChtimes(t *testing.T) {
	fs, done := newTestCOW(t)
	defer done()

	err := fs.Keep(Path("file1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}
	atime := time.Now().Truncate(1 * time.Second)
	mtime := atime.Add(-1 * time.Minute)
	err = fs.Chtimes(Path("file1"), atime, mtime)
	if err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	if err := checkTestCOW(fs, testTree()); err != nil {
		t.Error(err)
	}

	root := string(fs.fs.(*Local).root.Resolve(fs.wroot))
	fi, err := os.Lstat(filepath.Join(root, "file1"))
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if fi.ModTime().Truncate(1*time.Second) != mtime {
		t.Errorf("Chtimes mtime: got %v, want %v", fi.ModTime(), mtime)
	}
}

func newTestCOW(t *testing.T) (*COW, func()) {
	nows := now.Add(-1 * time.Hour).Format("2006-01-02T15-04-05.000000")

	// Create an old instance and the symlinks to it.
	lfs, done := newTestLocalAt(t, filepath.Join("test", nows))
	if err := os.Symlink(nows, string(lfs.root.Resolve("test").Resolve(latestPath))); err != nil {
		t.Fatalf("Symlink(%q) failed: %v", nows, err)
	}
	if err := os.Symlink(filepath.Join("test", nows), string(lfs.root.Resolve(latestPath))); err != nil {
		t.Fatalf("Symlink(%q) failed: %v", nows, err)
	}

	fs, err := NewCOW(lfs, "test", now)
	if err != nil {
		t.Fatalf("NewCow failed: %v", err)
	}
	return fs, done
}

func checkTestCOW(got *COW, want []*memDirEnt) error {
	return nil
}
