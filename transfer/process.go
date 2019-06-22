package transfer

import (
	"context"
	"os"
	"sort"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/tommie/fisy/fs"
	"golang.org/x/sync/errgroup"
)

// A process contains information about an in-progress transfer. While
// Run is executing, Stats can be used to get progress information.
type process struct {
	src          fs.ReadableFileSystem
	dest         fs.WriteableFileSystem
	ignoreFilter func(fs.Path) bool
	nconc        int

	stats    *ProcessStats
	transfer func(context.Context, *filePair) error
}

// Run performs a parallel file transfer. Only one Run should be
// executing for a process.
func (p *process) Run(ctx context.Context) error {
	fps, err := p.listDir(fs.Path("."))
	if err != nil {
		return err
	}

	return filePairPDFS(ctx, fps, p.process, p.nconc)
}

// process is invoked once per file or directory. For directories, it
// returns the children at the source. When this returns, the
// directory/file has been fully created.
func (p *process) process(ctx context.Context, fp *filePair) ([]*filePair, error) {
	atomic.AddUint32(&p.stats.InProgress, 1)
	defer atomic.AddUint32(&p.stats.InProgress, ^uint32(0))

	if fp.src != nil {
		if fp.src.IsDir() {
			atomic.AddUint64(&p.stats.SourceDirectories, 1)
		} else {
			atomic.AddUint64(&p.stats.SourceBytes, uint64(fp.src.Size()))
			atomic.AddUint64(&p.stats.SourceFiles, 1)
		}
	}

	isDir := fp.FileInfo().IsDir()
	filterPath := "/" + fp.path
	if isDir {
		filterPath += "/"
	}
	if p.ignoreFilter(filterPath) {
		if isDir {
			atomic.AddUint64(&p.stats.IgnoredDirectories, 1)
		} else {
			atomic.AddUint64(&p.stats.IgnoredFiles, 1)
		}
		glog.V(3).Infof("Ignored %q.", fp.path)
		return nil, nil
	}

	var fps []*filePair
	var eg errgroup.Group
	if fp.src != nil && fp.src.IsDir() {
		eg.Go(func() error {
			var err error
			fps, err = p.listDir(fp.path)
			return err
		})
	}
	eg.Go(func() error {
		err := p.transfer(ctx, fp)
		if err != nil {
			glog.Errorf("Failed to transfer %q: %v", fp.path, err)
			glog.V(1).Infof("Source: %+v\nDestination: %+v", fp.src, fp.dest)
		}
		return err
	})
	return fps, eg.Wait()
}

// listDir creates file pairs for the children of the given directory.
func (p *process) listDir(path fs.Path) ([]*filePair, error) {
	var eg errgroup.Group
	var srcfiles, destfiles []os.FileInfo
	eg.Go(func() error {
		var err error
		srcfiles, err = readdir(p.src, path)
		if err != nil {
			return err
		}
		sort.Slice(srcfiles, func(i, j int) bool { return srcfiles[i].Name() < srcfiles[j].Name() })
		return nil
	})
	eg.Go(func() error {
		var err error
		destfiles, err = readdir(p.dest, path)
		if err != nil && !fs.IsNotExist(err) {
			return err
		}
		sort.Slice(destfiles, func(i, j int) bool { return destfiles[i].Name() < destfiles[j].Name() })
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Join the two sorted lists.
	var fps []*filePair
	var i, j int
	for i < len(srcfiles) && j < len(destfiles) {
		sf := srcfiles[i]
		df := destfiles[j]
		var name string
		if sf.Name() < df.Name() {
			// New file.
			df = nil
			name = sf.Name()
			i++
		} else if sf.Name() > df.Name() {
			// Removed file.
			sf = nil
			name = df.Name()
			j++
		} else {
			// In both.
			name = sf.Name()
			i++
			j++
		}
		fps = append(fps, &filePair{path: path.Resolve(fs.Path(name)), src: sf, dest: df})
	}
	for ; i < len(srcfiles); i++ {
		f := srcfiles[i]
		fps = append(fps, &filePair{path: path.Resolve(fs.Path(f.Name())), src: f})
	}
	for ; j < len(destfiles); j++ {
		f := destfiles[j]
		fps = append(fps, &filePair{path: path.Resolve(fs.Path(f.Name())), dest: f})
	}

	// To reduce memory footprint, we want to work on files first,
	// since directories may add more in-memory data. Later files
	// in fps will be worked on earlier.
	sort.Slice(fps, func(i, j int) bool {
		idir := fps[i].FileInfo().IsDir()
		jdir := fps[j].FileInfo().IsDir()
		if idir != jdir {
			// If i is a directory, then it is "less".
			return idir
		}
		return fps[i].path < fps[j].path
	})

	return fps, nil
}

// readdir reads the contents of a directory.
func readdir(fs fs.ReadableFileSystem, path fs.Path) ([]os.FileInfo, error) {
	fr, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	return fr.Readdir()
}

// ProcessStats contains a snapshot of transfer process statistics.
type ProcessStats struct {
	InProgress uint32

	SourceBytes       uint64
	SourceFiles       uint64
	SourceDirectories uint64

	IgnoredFiles       uint64
	IgnoredDirectories uint64
}

// CopyFrom does atomic reads from source, and assigns to the receiver.
func (ps *ProcessStats) CopyFrom(src *ProcessStats) {
	ps.InProgress = atomic.LoadUint32(&src.InProgress)
	ps.SourceBytes = atomic.LoadUint64(&src.SourceBytes)
	ps.SourceFiles = atomic.LoadUint64(&src.SourceFiles)
	ps.SourceDirectories = atomic.LoadUint64(&src.SourceDirectories)
	ps.IgnoredFiles = atomic.LoadUint64(&src.IgnoredFiles)
	ps.IgnoredDirectories = atomic.LoadUint64(&src.IgnoredDirectories)
}
