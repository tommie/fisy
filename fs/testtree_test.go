package fs

import (
	"os"
	"strings"
	"syscall"
)

type memDirEnt struct {
	Name     string
	Mode     os.FileMode
	Content  string
	Children []*memDirEnt
}

var testUmask = func() os.FileMode {
	umask := os.FileMode(syscall.Umask(0))
	syscall.Umask(int(umask))
	return umask
}()

func testTree() []*memDirEnt {
	return []*memDirEnt{
		{"dir1", 0777 | os.ModeDir, "", []*memDirEnt{
			{"dir-empty", 0777 | os.ModeDir, "", []*memDirEnt{}},
			{"file-readonly", 0444, "content readonly\n", nil},
		}},
		{"dir-readonly", 0555 | os.ModeDir, "", []*memDirEnt{
			{"file-private", 0600, "content private\n", nil},
		}},
		{"dir-private", 0700 | os.ModeDir, "", []*memDirEnt{
			{"file2", 0666, "content 2\n", nil},
		}},
		{"file1", 0666, "content 1\n", nil},
		{"file-noperms", 0000, "content noperms\n", nil},
		{"symlink1", 0777 | os.ModeSymlink, "file1", nil},
		{"symlink-dangling", 0777 | os.ModeSymlink, "nothing", nil},
		{"hardlink1", 0666, "file1", nil},
		{"hardlink-symlink", 0777 | os.ModeSymlink, "symlink1", nil},
	}
}

func findMemDirEnt(tree []*memDirEnt, path string) *memDirEnt {
	for _, de := range tree {
		if path == de.Name {
			return de
		}
		if !strings.HasPrefix(path, de.Name+"/") {
			continue
		}
		return findMemDirEnt(de.Children, path[len(de.Name)+1:])
	}
	return nil
}

func removeMemDirEnt(tree *[]*memDirEnt, path string) {
	for i, de := range *tree {
		if path == de.Name {
			*tree = append((*tree)[:i], (*tree)[i+1:]...)
			return
		}
		if !strings.HasPrefix(path, de.Name+"/") {
			continue
		}
		removeMemDirEnt(&de.Children, path[len(de.Name)+1:])
	}
}
