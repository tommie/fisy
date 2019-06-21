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

var localIsAWriteableFileSystem WriteableFileSystem = &Local{}

func TestLocalOpen(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	fr, err := lfs.Open(Path("file1"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := fr.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestLocalOpenFailsNoPerms(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	_, err := lfs.Open(Path("file-noperms"))
	if !os.IsPermission(err) {
		t.Fatalf("Open error: got %v, want EPERM", err)
	}
}

func TestLocalFileReader(t *testing.T) {
	t.Run("Read", func(t *testing.T) {
		t.Parallel()

		lfs, done := newTestLocal(t)
		defer done()

		fr, err := lfs.Open(Path("file1"))
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

		lfs, done := newTestLocal(t)
		defer done()

		fr, err := lfs.Open(Path("dir1"))
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

		lfs, done := newTestLocal(t)
		defer done()

		fr, err := lfs.Open(Path("file1"))
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

func TestLocalReadlink(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	got, err := lfs.Readlink(Path("symlink1"))
	if err != nil {
		t.Fatalf("Readlink failed: %v", err)
	}

	if want := Path("file1"); got != want {
		t.Errorf("Readlink: got %q, want %q", got, want)
	}
}

func TestLocalStat(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	syscallStatfs = func(path string, sfs *syscall.Statfs_t) error {
		if path != string(lfs.root) {
			t.Errorf("Statfs path: got %q, want %q", path, lfs.root)
		}
		sfs.Frsize = 3
		sfs.Bavail = 5
		return nil
	}
	defer func() {
		syscallStatfs = syscall.Statfs
	}()

	got, err := lfs.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if want := 3 * 5; got.FreeSpace != uint64(want) {
		t.Fatalf("Stat FreeSpace: got %v, want %v", got.FreeSpace, want)
	}
}

func TestLocalCreate(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	fw, err := lfs.Create(Path("file-create"))
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

	if err := checkTestLocal(lfs, tree); err != nil {
		t.Error(err)
	}
}

func TestLocalFileWriter(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	fw, err := lfs.Create(Path("file-create"))
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

	if err := checkTestLocal(lfs, tree); err != nil {
		t.Error(err)
	}
}

func TestLocalKeep(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	// Does nothing.
	err := lfs.Keep(Path("file1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}

	if err := checkTestLocal(lfs, testTree()); err != nil {
		t.Error(err)
	}
}

func TestLocalMkdir(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	err := lfs.Mkdir(Path("dir-make"), 0700, os.Geteuid(), os.Geteuid())
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name: "dir-make",
		Mode: 0700 | os.ModeDir,
	})

	if err := checkTestLocal(lfs, tree); err != nil {
		t.Error(err)
	}
}

func TestLocalMkdirCleansUpOnFailure(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	err := lfs.Mkdir(Path("dir-make"), 0700, 1234, 1234)
	if err == nil {
		t.Fatal("Mkdir error: got nil, want EPERM")
	}

	if err := checkTestLocal(lfs, testTree()); err != nil {
		t.Error(err)
	}
}

func TestLocalLink(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	err := lfs.Link(Path("file1"), Path("hardlink-file1"))
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name:    "hardlink-file1",
		Mode:    0666,
		Content: "file1",
	})

	if err := checkTestLocal(lfs, tree); err != nil {
		t.Error(err)
	}
}

func TestLocalSymlink(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	err := lfs.Symlink(Path("file1"), Path("symlink-create"))
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name:    "symlink-create",
		Mode:    0777 | os.ModeSymlink,
		Content: "file1",
	})

	if err := checkTestLocal(lfs, tree); err != nil {
		t.Error(err)
	}
}

func TestLocalRename(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	err := lfs.Rename(Path("file1"), Path("file-rename"))
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	tree := testTree()
	findMemDirEnt(tree, "file1").Name = "file-rename"
	findMemDirEnt(tree, "hardlink1").Content = "file-rename"

	if err := checkTestLocal(lfs, tree); err != nil {
		t.Error(err)
	}
}

func TestLocalRemoveAll(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		t.Parallel()

		lfs, done := newTestLocal(t)
		defer done()

		err := lfs.RemoveAll(Path("hardlink1"))
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "hardlink1")

		if err := checkTestLocal(lfs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dir", func(t *testing.T) {
		t.Parallel()

		lfs, done := newTestLocal(t)
		defer done()

		err := lfs.RemoveAll(Path("dir1"))
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "dir1")

		if err := checkTestLocal(lfs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dir-private", func(t *testing.T) {
		t.Parallel()

		lfs, done := newTestLocal(t)
		defer done()

		err := lfs.RemoveAll(Path("dir-private"))
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "dir-private")

		if err := checkTestLocal(lfs, tree); err != nil {
			t.Error(err)
		}
	})
}

func TestLocalRemove(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		t.Parallel()

		lfs, done := newTestLocal(t)
		defer done()

		err := lfs.Remove(Path("hardlink1"))
		if err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "hardlink1")

		if err := checkTestLocal(lfs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dir", func(t *testing.T) {
		t.Parallel()

		lfs, done := newTestLocal(t)
		defer done()

		err := lfs.Remove(Path("dir1/dir-empty"))
		if err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "dir1/dir-empty")

		if err := checkTestLocal(lfs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dirFailsOnNonEmpty", func(t *testing.T) {
		t.Parallel()

		lfs, done := newTestLocal(t)
		defer done()

		err := lfs.Remove(Path("dir1"))
		if err == nil || !strings.Contains(err.Error(), "not empty") {
			t.Fatalf("Remove error: got %v, want ENOTEMPTY", err)
		}

		if err := checkTestLocal(lfs, testTree()); err != nil {
			t.Error(err)
		}
	})
}

func TestLocalChmod(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	err := lfs.Chmod(Path("file1"), 0600)
	if err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	tree := testTree()
	findMemDirEnt(tree, "file1").Mode = 0600
	findMemDirEnt(tree, "hardlink1").Mode = 0600

	if err := checkTestLocal(lfs, tree); err != nil {
		t.Error(err)
	}
}

func TestLocalLchown(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	osLchown = func(path string, uid, gid int) error {
		if want := filepath.Join(string(lfs.root), "file1"); path != want {
			t.Errorf("Lchown path: got %q, want %q", path, want)
		}
		return nil
	}
	defer func() {
		osLchown = os.Lchown
	}()

	err := lfs.Lchown(Path("file1"), 42, 43)
	if err != nil {
		t.Fatalf("Lchown failed: %v", err)
	}

	if err := checkTestLocal(lfs, testTree()); err != nil {
		t.Error(err)
	}
}

func TestLocalChtimes(t *testing.T) {
	lfs, done := newTestLocal(t)
	defer done()

	atime := time.Now().Truncate(1 * time.Second)
	mtime := atime.Add(-1 * time.Minute)
	err := lfs.Chtimes(Path("file1"), atime, mtime)
	if err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	if err := checkTestLocal(lfs, testTree()); err != nil {
		t.Error(err)
	}

	fi, err := os.Lstat(filepath.Join(string(lfs.root), "file1"))
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if fi.ModTime().Truncate(1*time.Second) != mtime {
		t.Errorf("Chtimes mtime: got %v, want %v", fi.ModTime(), mtime)
	}
}

func newTestLocal(t *testing.T) (*Local, func()) {
	tmpd, err := ioutil.TempDir("", "localfs-")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	var ok bool
	done := func() {
		var rec func(string) error
		rec = func(path string) error {
			fi, err := os.Lstat(path)
			if err != nil {
				return err
			}
			if fi.Mode()&0200 == 0 {
				if err := os.Chmod(path, 0700); err != nil {
					t.Error(err)
				}
			}
			if fi.IsDir() {
				fis, err := ioutil.ReadDir(path)
				if err != nil {
					return err
				}
				for _, fi := range fis {
					if err := rec(filepath.Join(path, fi.Name())); err != nil {
						t.Error(err)
					}
				}
			}
			return nil
		}
		rec(tmpd)
		os.RemoveAll(tmpd)
	}
	defer func() {
		if !ok {
			done()
		}
	}()

	var rec func(string, *memDirEnt) error
	rec = func(parent string, de *memDirEnt) error {
		path := filepath.Join(parent, de.Name)
		switch {
		case de.Mode.IsDir():
			if err := os.Mkdir(path, (de.Mode&^os.ModeDir)|0700); err != nil {
				return err
			}
			for _, de := range de.Children {
				if err := rec(path, de); err != nil {
					return err
				}
			}
			return os.Chmod(path, de.Mode&^os.ModeDir)

		case de.Mode&os.ModeSymlink != 0:
			return os.Symlink(de.Content, path)

		case strings.HasPrefix(de.Name, "hardlink"):
			return os.Link(filepath.Join(parent, de.Content), path)

		default:
			f, err := os.Create(path)
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := fmt.Fprint(f, de.Content); err != nil {
				return err
			}
			return f.Chmod(de.Mode)
		}
	}

	for _, de := range testTree() {
		if err := rec(tmpd, de); err != nil {
			done()
			t.Fatalf("testTree creation failed: %v", err)
		}
	}

	ok = true
	return NewLocal(tmpd), done
}

func checkTestLocal(got *Local, want []*memDirEnt) error {
	var rec func(string, []os.FileInfo, []*memDirEnt) error
	rec = func(root string, gots []os.FileInfo, wants []*memDirEnt) error {
		sort.Slice(gots, func(i, j int) bool { return gots[i].Name() < gots[j].Name() })
		sort.Slice(wants, func(i, j int) bool { return wants[i].Name < wants[j].Name })

		if len(gots) != len(wants) {
			return fmt.Errorf("different number of file nodes at %q: got %v, want %v", root, len(gots), len(wants))
		}
		for i, got := range gots {
			p := filepath.Join(root, got.Name())
			want := wants[i]
			if got.Name() != want.Name {
				return fmt.Errorf("mismatching file names at %q: got %q, want %q", root, got.Name(), want.Name)
			}
			if got.Mode() != want.Mode {
				return fmt.Errorf("mismatching file modes at %q: got %v, want %v", p, got.Mode(), want.Mode)
			}
			switch {
			case got.IsDir():
				fis, err := ioutil.ReadDir(p)
				if err != nil {
					return err
				}
				if err := rec(p, fis, want.Children); err != nil {
					return err
				}

			case got.Mode()&os.ModeSymlink != 0:
				s, err := os.Readlink(p)
				if err != nil {
					return err
				}
				if s != want.Content {
					return fmt.Errorf("mismatching symlink content at %q: got %q, want %q", p, s, want.Content)
				}

			default:
				if strings.HasPrefix(got.Name(), "hardlink") {
					want2 := findMemDirEnt(wants, want.Content)
					if want2 == nil {
						return fmt.Errorf("invalid wanted hardlink: %v", want)
					}
					want = want2
				}
				if got.Mode()&0444 != 0 {
					bs, err := ioutil.ReadFile(p)
					if err != nil {
						return err
					}
					if string(bs) != want.Content {
						return fmt.Errorf("mismatching file content at %q: got %q, want %q", p, string(bs), want.Content)
					}
				}
			}
		}
		return nil
	}

	fis, err := ioutil.ReadDir(string(got.root))
	if err != nil {
		return err
	}
	return rec(string(got.root), fis, want)
}
