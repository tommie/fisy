package transfer

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/tommie/fisy/fs"
)

func TestProcessRun(t *testing.T) {
	ctx := context.Background()

	p := newTestProcess()

	var transfers []*filePair
	p.transfer = func(ctx context.Context, fp *filePair) error {
		transfers = append(transfers, fp)
		return nil
	}

	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if want := 9; len(transfers) != want {
		t.Errorf("transfers: got %+v, want len %v", transfers, want)
	}

	if got, want := int(p.stats.InProgress), 0; got != want {
		t.Errorf("Run stats.InProgress: got %v, want %v", got, want)
	}

	if got, want := int(p.stats.SourceDirectories), 2; got != want {
		t.Errorf("Run stats.SourceDirectories: got %v, want %v", got, want)
	}

	if got, want := int(p.stats.SourceFiles), 5; got != want {
		t.Errorf("Run stats.SourceFiles: got %v, want %v", got, want)
	}

	if got, want := int(p.stats.SourceBytes), 5*42; got != want {
		t.Errorf("Run stats.SourceBytes: got %v, want %v", got, want)
	}
}

func TestProcessIgnoreFilter(t *testing.T) {
	ctx := context.Background()

	t.Run("dir", func(t *testing.T) {
		p := newTestProcess()

		p.ignoreFilter = func(path fs.Path) bool {
			return path == fs.Path("/dir1/")
		}

		var ntransfers int
		p.transfer = func(ctx context.Context, fp *filePair) error {
			ntransfers++
			return nil
		}

		_, err := p.process(ctx, &filePair{path: fs.Path("dir1"), src: &fakeListingFileInfo{name: "dir1", mode: os.ModeDir}})
		if err != nil {
			t.Fatalf("process failed: %v", err)
		}

		if want := 0; ntransfers != want {
			t.Errorf("ntransfers: got %v, want len %v", ntransfers, want)
		}

		if got, want := int(p.stats.IgnoredDirectories), 1; got != want {
			t.Errorf("Run stats.IgnoredDirectories: got %v, want %v", got, want)
		}
		if got, want := int(p.stats.IgnoredFiles), 0; got != want {
			t.Errorf("Run stats.IgnoredFiles: got %v, want %v", got, want)
		}
	})

	t.Run("file", func(t *testing.T) {
		p := newTestProcess()

		p.ignoreFilter = func(path fs.Path) bool {
			return path == fs.Path("/file1")
		}

		var ntransfers int
		p.transfer = func(ctx context.Context, fp *filePair) error {
			ntransfers++
			return nil
		}

		_, err := p.process(ctx, &filePair{path: fs.Path("file1"), src: &fakeListingFileInfo{name: "file1", mode: 0}})
		if err != nil {
			t.Fatalf("process failed: %v", err)
		}

		if want := 0; ntransfers != want {
			t.Errorf("ntransfers: got %v, want len %v", ntransfers, want)
		}

		if got, want := int(p.stats.IgnoredDirectories), 0; got != want {
			t.Errorf("Run stats.IgnoredDirectories: got %v, want %v", got, want)
		}
		if got, want := int(p.stats.IgnoredFiles), 1; got != want {
			t.Errorf("Run stats.IgnoredFiles: got %v, want %v", got, want)
		}
	})
}

func TestProcessPropagatesTransferError(t *testing.T) {
	errMocked := errors.New("mocked")

	ctx := context.Background()

	t.Run("dir", func(t *testing.T) {
		p := newTestProcess()

		p.transfer = func(ctx context.Context, fp *filePair) error {
			return errMocked
		}

		_, err := p.process(ctx, &filePair{path: fs.Path("dir1"), src: &fakeListingFileInfo{name: "dir1", mode: os.ModeDir}})
		if err != errMocked {
			t.Fatalf("process error: got %v, want %v", err, errMocked)
		}

		if got, want := int(p.stats.FailedDirectories), 1; got != want {
			t.Errorf("Run stats.FailedDirectories: got %v, want %v", got, want)
		}
		if got, want := int(p.stats.FailedFiles), 0; got != want {
			t.Errorf("Run stats.FailedFiles: got %v, want %v", got, want)
		}
	})

	t.Run("file", func(t *testing.T) {
		p := newTestProcess()

		p.transfer = func(ctx context.Context, fp *filePair) error {
			return errMocked
		}

		_, err := p.process(ctx, &filePair{path: fs.Path("file1"), src: &fakeListingFileInfo{name: "file1"}})
		if err != errMocked {
			t.Fatalf("process error: got %v, want %v", err, errMocked)
		}

		if got, want := int(p.stats.FailedDirectories), 0; got != want {
			t.Errorf("Run stats.FailedDirectories: got %v, want %v", got, want)
		}
		if got, want := int(p.stats.FailedFiles), 1; got != want {
			t.Errorf("Run stats.FailedFiles: got %v, want %v", got, want)
		}
	})
}

func TestProcessListDir(t *testing.T) {
	p := newTestProcess()

	t.Run("same", func(t *testing.T) {
		fps, err := p.listDir(fs.Path("."))
		if err != nil {
			t.Fatalf("listDir failed: %v", err)
		}

		var got []fs.Path
		for _, fp := range fps {
			got = append(got, fp.path)
		}
		if want := []fs.Path{"dir1", "dir2", "file1"}; !reflect.DeepEqual(got, want) {
			t.Errorf("listDir: got %+v, want %+v", got, want)
		}
	})

	t.Run("created", func(t *testing.T) {
		fps, err := p.listDir(fs.Path("dir1"))
		if err != nil {
			t.Fatalf("listDir failed: %v", err)
		}

		var got []fs.Path
		for _, fp := range fps {
			got = append(got, fp.path)
		}
		if want := []fs.Path{"dir1/file-new", "dir1/file2", "dir1/new-file"}; !reflect.DeepEqual(got, want) {
			t.Errorf("listDir: got %+v, want %+v", got, want)
		}
	})

	t.Run("removed", func(t *testing.T) {
		fps, err := p.listDir(fs.Path("dir2"))
		if err != nil {
			t.Fatalf("listDir failed: %v", err)
		}

		var got []fs.Path
		for _, fp := range fps {
			got = append(got, fp.path)
		}
		if want := []fs.Path{"dir2/file-removed", "dir2/file3", "dir2/removed-file"}; !reflect.DeepEqual(got, want) {
			t.Errorf("listDir: got %+v, want %+v", got, want)
		}
	})
}

func TestProcessStats(t *testing.T) {
	want := ProcessStats{
		InProgress: 1,
		SourceBytes: 2,
		SourceFiles: 3,
		SourceDirectories: 4,
		IgnoredFiles: 5,
		IgnoredDirectories: 6,
		FailedFiles: 7,
		FailedDirectories: 8,
	}

	var got ProcessStats
	got.CopyFrom(&want)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("CopyFrom: got %+v, want %+v", got, want)
	}
}

func newTestProcess() process {
	return process{
		src: &fakeListingFileSystem{
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
		},
		dest: &fakeListingFileSystem{
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
		},
		ignoreFilter: func(fs.Path) bool { return false },
		nconc:        1,

		stats:    &ProcessStats{},
		transfer: func(context.Context, *filePair) error { return nil },
	}
}

type fakeListingFileSystem struct {
	fs.WriteableFileSystem

	fis map[fs.Path][]os.FileInfo
}

func (fs *fakeListingFileSystem) Open(path fs.Path) (fs.FileReader, error) {
	return &fakeListingFileReader{fis: fs.fis[path]}, nil
}

type fakeListingFileReader struct {
	fs.FileReader

	fis []os.FileInfo
}

func (fr *fakeListingFileReader) Close() error {
	return nil
}

func (fr *fakeListingFileReader) Readdir() ([]os.FileInfo, error) {
	return fr.fis, nil
}

type fakeListingFileInfo struct {
	os.FileInfo

	name string
	mode os.FileMode
}

func (fi *fakeListingFileInfo) Name() string      { return fi.name }
func (fi *fakeListingFileInfo) Mode() os.FileMode { return fi.mode }
func (fi *fakeListingFileInfo) Size() int64       { return 42 }
