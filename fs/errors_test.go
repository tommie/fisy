package fs

import (
	"os"
	"testing"

	"github.com/pkg/sftp"
)

func TestIsErrors(t *testing.T) {
	errSFTPExist := &sftp.StatusError{Code: 11}
	errSFTPNotExist := &sftp.StatusError{Code: 2}
	errSFTPPermission := &sftp.StatusError{Code: 3}

	tsts := []struct {
		Name string
		Func func(error) bool
		Err  error
		Want bool
	}{
		{"IsExist", IsExist, os.ErrExist, true},
		{"IsExist", IsExist, &os.PathError{Err: os.ErrExist}, true},
		{"IsExist", IsExist, &os.LinkError{Err: os.ErrExist}, true},
		{"IsExist", IsExist, os.ErrNotExist, false},

		{"IsNotExist", IsNotExist, os.ErrNotExist, true},
		{"IsNotExist", IsNotExist, &os.PathError{Err: os.ErrNotExist}, true},
		{"IsNotExist", IsNotExist, &os.LinkError{Err: os.ErrNotExist}, true},
		{"IsNotExist", IsNotExist, os.ErrExist, false},

		{"IsPermission", IsPermission, os.ErrPermission, true},
		{"IsPermission", IsPermission, &os.PathError{Err: os.ErrPermission}, true},
		{"IsPermission", IsPermission, &os.LinkError{Err: os.ErrPermission}, true},
		{"IsPermission", IsPermission, os.ErrExist, false},

		{"IsExist", IsExist, errSFTPExist, true},
		{"IsExist", IsExist, &os.PathError{Err: errSFTPExist}, true},
		{"IsExist", IsExist, &os.LinkError{Err: errSFTPExist}, true},

		{"IsNotExist", IsNotExist, errSFTPNotExist, true},
		{"IsNotExist", IsNotExist, &os.PathError{Err: errSFTPNotExist}, true},
		{"IsNotExist", IsNotExist, &os.LinkError{Err: errSFTPNotExist}, true},

		{"IsPermission", IsPermission, errSFTPPermission, true},
		{"IsPermission", IsPermission, &os.PathError{Err: errSFTPPermission}, true},
		{"IsPermission", IsPermission, &os.LinkError{Err: errSFTPPermission}, true},
	}
	for _, tst := range tsts {
		tst := tst
		t.Run(tst.Err.Error(), func(t *testing.T) {
			if tst.Func(tst.Err) != tst.Want {
				t.Fatalf("%v: got %v, want %v", tst.Name, !tst.Want, tst.Want)
			}
		})
	}
}
