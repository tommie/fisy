package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tommie/fisy/remote/testutil"
)

var sftpIsAWriteableFileSystem WriteableFileSystem = &SFTP{}

func TestSFTPOpen(t *testing.T) {
	sfs, done := newTestSFTP(t)
	defer done()

	fr, err := sfs.Open(Path("file1"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := fr.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestSFTPFileReader(t *testing.T) {
	t.Run("Read", func(t *testing.T) {
		t.Parallel()

		sfs, done := newTestSFTP(t)
		defer done()

		fr, err := sfs.Open(Path("file1"))
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

		sfs, done := newTestSFTP(t)
		defer done()

		fr, err := sfs.Open(Path("dir1"))
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

		sfs, done := newTestSFTP(t)
		defer done()

		fr, err := sfs.Open(Path("file1"))
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

func TestSFTPReadlink(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	got, err := fs.Readlink(Path("symlink1"))
	if err != nil {
		t.Fatalf("Readlink failed: %v", err)
	}

	if want := Path("file1"); got != want {
		t.Errorf("Readlink: got %q, want %q", got, want)
	}
}

func TestSFTPStat(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	got, err := fs.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if got.FreeSpace == 0 {
		t.Fatalf("Stat FreeSpace: got %v, want != 0", got.FreeSpace)
	}
}

func TestSFTPCreate(t *testing.T) {
	fs, done := newTestSFTP(t)
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

	if err := checkTestSFTP(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestSFTPFileWriter(t *testing.T) {
	fs, done := newTestSFTP(t)
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

	if err := checkTestSFTP(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestSFTPKeep(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	// Does nothing.
	err := fs.Keep(Path("file1"))
	if err != nil {
		t.Fatalf("Keep failed: %v", err)
	}

	if err := checkTestSFTP(fs, testTree()); err != nil {
		t.Error(err)
	}
}

func TestSFTPMkdir(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	err := fs.Mkdir(Path("dir-make"), 0700, os.Geteuid(), os.Geteuid())
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name: "dir-make",
		Mode: 0700 | os.ModeDir,
	})

	if err := checkTestSFTP(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestSFTPMkdirCleansUpOnFailure(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	err := fs.Mkdir(Path("dir-make"), 0700, 1234, 1234)
	if err == nil {
		t.Fatal("Mkdir error: got nil, want EPERM")
	}

	if err := checkTestSFTP(fs, testTree()); err != nil {
		t.Error(err)
	}
}

func TestSFTPLink(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	err := fs.Link(Path("file1"), Path("hardlink-file1"))
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}

	tree := append(testTree(), &memDirEnt{
		Name:    "hardlink-file1",
		Mode:    0666,
		Content: "file1",
	})

	if err := checkTestSFTP(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestSFTPSymlink(t *testing.T) {
	fs, done := newTestSFTP(t)
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

	if err := checkTestSFTP(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestSFTPRename(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	err := fs.Rename(Path("file1"), Path("file-rename"))
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	tree := testTree()
	findMemDirEnt(tree, "file1").Name = "file-rename"
	findMemDirEnt(tree, "hardlink1").Content = "file-rename"

	if err := checkTestSFTP(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestSFTPRemoveAll(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestSFTP(t)
		defer done()

		err := fs.RemoveAll(Path("hardlink1"))
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "hardlink1")

		if err := checkTestSFTP(fs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dir", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestSFTP(t)
		defer done()

		err := fs.RemoveAll(Path("dir1"))
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "dir1")

		if err := checkTestSFTP(fs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dir-private", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestSFTP(t)
		defer done()

		err := fs.RemoveAll(Path("dir-private"))
		if err != nil {
			t.Fatalf("RemoveAll failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "dir-private")

		if err := checkTestSFTP(fs, tree); err != nil {
			t.Error(err)
		}
	})
}

func TestSFTPRemove(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestSFTP(t)
		defer done()

		err := fs.Remove(Path("hardlink1"))
		if err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "hardlink1")

		if err := checkTestSFTP(fs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dir", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestSFTP(t)
		defer done()

		err := fs.Remove(Path("dir1/dir-empty"))
		if err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		tree := testTree()
		removeMemDirEnt(&tree, "dir1/dir-empty")

		if err := checkTestSFTP(fs, tree); err != nil {
			t.Error(err)
		}
	})

	t.Run("dirFailsOnNonEmpty", func(t *testing.T) {
		t.Parallel()

		fs, done := newTestSFTP(t)
		defer done()

		err := fs.Remove(Path("dir1"))
		if !strings.Contains(err.Error(), "not empty") {
			t.Fatalf("Remove error: got %v, want ENOTEMPTY", err)
		}

		if err := checkTestSFTP(fs, testTree()); err != nil {
			t.Error(err)
		}
	})
}

func TestSFTPChmod(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	err := fs.Chmod(Path("file1"), 0600)
	if err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	tree := testTree()
	findMemDirEnt(tree, "file1").Mode = 0600
	findMemDirEnt(tree, "hardlink1").Mode = 0600

	if err := checkTestSFTP(fs, tree); err != nil {
		t.Error(err)
	}
}

func TestSFTPLchown(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	err := fs.Lchown(Path("file1"), os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatalf("Lchown failed: %v", err)
	}

	if err := checkTestSFTP(fs, testTree()); err != nil {
		t.Error(err)
	}
}

func TestSFTPChtimes(t *testing.T) {
	fs, done := newTestSFTP(t)
	defer done()

	atime := time.Now().Truncate(1 * time.Second)
	mtime := atime.Add(-1 * time.Minute)
	err := fs.Chtimes(Path("file1"), atime, mtime)
	if err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	if err := checkTestSFTP(fs, testTree()); err != nil {
		t.Error(err)
	}

	fi, err := os.Lstat(filepath.Join(string(fs.root), "file1"))
	if err != nil {
		t.Fatalf("Lstat failed: %v", err)
	}

	if fi.ModTime().Truncate(1*time.Second) != mtime {
		t.Errorf("Chtimes mtime: got %v, want %v", fi.ModTime(), mtime)
	}
}

func newTestSFTP(t *testing.T) (*SFTP, func()) {
	mc, stop, err := testutil.NewTestSFTPClient()
	if err != nil {
		t.Fatalf("NewTestSFTPClient failed: %v", err)
	}

	fs, done := newTestLocal(t)
	return NewSFTP(mc, fs.root), func() {
		stop()
		done()
	}
}

func checkTestSFTP(got *SFTP, want []*memDirEnt) error {
	client := got.client

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
				fis, err := client.ReadDir(p)
				if err != nil {
					return err
				}
				if err := rec(p, fis, want.Children); err != nil {
					return err
				}

			case got.Mode()&os.ModeSymlink != 0:
				s, err := client.ReadLink(p)
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
					f, err := client.Open(p)
					if err != nil {
						return err
					}
					bs, err := ioutil.ReadAll(f)
					f.Close()
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

	fis, err := client.ReadDir(string(got.root))
	if err != nil {
		return err
	}
	return rec(string(got.root), fis, want)
}
