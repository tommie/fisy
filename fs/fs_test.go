package fs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/tommie/fisy/remote/testutil"
)

func TestPath(t *testing.T) {
	t.Run("base", func(t *testing.T) {
		got := Path("dir/base").Base()
		if want := Path("base"); got != want {
			t.Errorf("Base: got %q, want %q", got, want)
		}
	})

	t.Run("dir", func(t *testing.T) {
		got := Path("dir/base").Dir()
		if want := Path("dir"); got != want {
			t.Errorf("Dir: got %q, want %q", got, want)
		}
	})

	t.Run("resolveRelative", func(t *testing.T) {
		got := Path("dir").Resolve("dir2/base")
		if want := Path("dir/dir2/base"); got != want {
			t.Errorf("Resolve: got %q, want %q", got, want)
		}
	})

	t.Run("resolveAbsolute", func(t *testing.T) {
		got := Path("dir").Resolve("/dir2/base")
		if want := Path("dir/dir2/base"); got != want {
			t.Errorf("Resolve: got %q, want %q", got, want)
		}
	})
}

func TestUidGidFromFileInfo(t *testing.T) {
	t.Run("os", func(t *testing.T) {
		tmpf, err := ioutil.TempFile("", "fs_test-")
		if err != nil {
			t.Fatalf("TempFile failed: %v", err)
		}
		defer tmpf.Close()
		defer os.Remove(tmpf.Name())

		fi, err := tmpf.Stat()
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}

		uid, gid, err := uidGidFromFileInfo(fi)
		if err != nil {
			t.Fatalf("uidGidFromFileInfo failed: %v", err)
		}

		if want := os.Geteuid(); uid != want {
			t.Errorf("uidGidFromFileInfo uid: got %v, want %v", uid, want)
		}
		if want := os.Getegid(); gid != want {
			t.Errorf("uidGidFromFileInfo gid: got %v, want %v", gid, want)
		}
	})

	t.Run("sftp", func(t *testing.T) {
		sftp, done, err := testutil.NewTestSFTPClient()
		if err != nil {
			t.Fatalf("NewTestSFTPClient failed: %v", err)
		}
		defer done()

		tmpf, err := ioutil.TempFile("", "fs_test-")
		if err != nil {
			t.Fatalf("TempFile failed: %v", err)
		}
		defer tmpf.Close()
		defer os.Remove(tmpf.Name())

		fi, err := sftp.Stat(tmpf.Name())
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}

		uid, gid, err := uidGidFromFileInfo(fi)
		if err != nil {
			t.Fatalf("uidGidFromFileInfo failed: %v", err)
		}

		if want := os.Geteuid(); uid != want {
			t.Errorf("uidGidFromFileInfo uid: got %v, want %v", uid, want)
		}
		if want := os.Getegid(); gid != want {
			t.Errorf("uidGidFromFileInfo gid: got %v, want %v", gid, want)
		}
	})
}
