package fs

import (
	"os"

	"github.com/pkg/sftp"
)

func IsExist(err error) bool {
	if os.IsExist(err) {
		return true
	}
	if e, ok := err.(*os.PathError); ok {
		err = e.Err
	}
	if e, ok := err.(*os.LinkError); ok {
		err = e.Err
	}
	if e, ok := err.(*sftp.StatusError); ok {
		return e.Code == 11 // sftp.ssh_FX_FILE_ALREADY_EXISTS
	}
	return false
}

func IsNotExist(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	if e, ok := err.(*os.PathError); ok {
		err = e.Err
	}
	if e, ok := err.(*os.LinkError); ok {
		err = e.Err
	}
	if e, ok := err.(*sftp.StatusError); ok {
		return e.Code == 2 // sftp.ssh_FX_NO_SUCH_FILE
	}
	return false
}

func IsPermission(err error) bool {
	if os.IsPermission(err) {
		return true
	}
	if e, ok := err.(*os.PathError); ok {
		err = e.Err
	}
	if e, ok := err.(*os.LinkError); ok {
		err = e.Err
	}
	if e, ok := err.(*sftp.StatusError); ok {
		return e.Code == 3 // sftp.ssh_FX_PERMISSION_DENIED
	}
	return false
}
