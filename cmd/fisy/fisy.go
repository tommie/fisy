// Command fisy is the main entry point for users.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/sftp"
	"github.com/sabhiram/go-gitignore"
	"github.com/tommie/fisy/fs"
	"github.com/tommie/fisy/transfer"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

var (
	ignore = flag.String("ignore", "", "filter to apply to ignore some files")
)

func main() {
	ctx := context.Background()

	flag.Parse()

	if flag.NArg() != 2 {
		glog.Error("expected two arguments")
		os.Exit(1)
	}

	if err := runUpload(ctx, flag.Arg(0), flag.Arg(1), *ignore); err != nil {
		glog.Error(err)
		os.Exit(10)
	}
}

func runUpload(ctx context.Context, srcSpec, destSpec, ignoreSpec string) (rerr error) {
	filter, err := parseIgnoreFilter(ignoreSpec)
	if err != nil {
		return err
	}

	src, srcClose, err := makeFileSystem(srcSpec)
	if err != nil {
		return err
	}
	defer func() {
		srcClose(rerr)
	}()

	dest, destClose, err := makeFileSystem(destSpec)
	if err != nil {
		return err
	}
	defer func() {
		destClose(rerr)
	}()

	start := time.Now()
	u := transfer.NewUpload(dest, src, transfer.WithIgnoreFilter(filter))
	stopCh := make(chan struct{})

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			showStats(u)

			select {
			case <-stopCh:
				return
			case <-t.C:
				// Continue
			}
		}
	}()

	if err := u.Run(ctx); err != nil {
		return err
	}
	close(stopCh)

	showStats(u)
	fmt.Println()

	glog.Infof("All done in %v: %+v", time.Now().Sub(start), u.Stats())

	return nil
}

func showStats(u *transfer.Upload) {
	st := u.Stats()
	fmt.Printf("\033[2K%5d / %7v: %s\033[1G", st.SourceFiles, storageBytes(st.UploadedBytes), st.LastPath())
}

type storageBytes uint64

var storageBytesUnits = []string{
	"B", "kiB", "MiB", "GiB", "PiB",
}

func (v storageBytes) String() string {
	f := float64(v)
	for _, unit := range storageBytesUnits {
		if f == 0 {
			return fmt.Sprintf("%.0f %s", f, unit)
		} else if f < 16 {
			return fmt.Sprintf("%.1f %s", f, unit)
		} else if f < 512 {
			return fmt.Sprintf("%.0f %s", f, unit)
		}
		f /= 1024
	}
	return fmt.Sprintf("%.0f EiB", f)
}

func makeFileSystem(s string) (fs.WriteableFileSystem, func(error) error, error) {
	u, err := parseFileSystemSpec(s)
	if err != nil {
		return nil, nil, err
	}
	return makeFileSystemFromURL(u)
}

func parseFileSystemSpec(s string) (*url.URL, error) {
	if strings.Contains(s, "://") {
		return url.Parse(s)
	}

	if strings.Contains(s, ":") {
		ss := strings.SplitN(s, ":", 2)
		return &url.URL{Scheme: "ssh", Host: ss[0], Path: ss[1]}, nil
	}

	return &url.URL{Scheme: "file", Path: s}, nil
}

func makeFileSystemFromURL(u *url.URL) (fs.WriteableFileSystem, func(error) error, error) {
	if strings.HasPrefix(u.Scheme, "cow+") {
		uu := *u
		uu.Scheme = uu.Scheme[4:]

		raw, close, err := makeFileSystemFromURL(&uu)
		if err != nil {
			return nil, nil, err
		}
		host, err := os.Hostname()
		if err != nil {
			return nil, nil, err
		}
		cfs, err := fs.NewCOWFileSystem(raw, host, time.Now())
		return cfs, func(err error) error {
			if err == nil {
				if err := cfs.Finish(); err != nil {
					return err
				}
			}
			return close(err)
		}, err
	}

	switch u.Scheme {
	case "file":
		return fs.NewLocalFileSystem(u.Path), func(error) error { return nil }, nil

	case "rsh", "ssh":
		host := u.Host
		if u.Port() == "" {
			// Note that ":ssh" doesn't work with the sftp
			// library. It would try to match a host key
			// named "host:ssh" instead of canonicalizing
			// it to just "host".
			host += ":22"
		}

		wfs, close, err := newSFTPFileSystem(host, u.Path)
		if err != nil {
			return nil, nil, err
		}
		return wfs, func(error) error { return close() }, nil

	default:
		return nil, nil, fmt.Errorf("unknown URL scheme: %s", u.Scheme)
	}
}

func newSFTPFileSystem(host, path string) (fs.WriteableFileSystem, func() error, error) {
	hkcb, err := knownhosts.New(os.ExpandEnv("$HOME/.ssh/known_hosts"))
	if err != nil {
		return nil, nil, err
	}
	agentConn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
	if err != nil {
		return nil, nil, err
	}
	cfg := ssh.ClientConfig{
		User: os.Getenv("LOGNAME"),
		Auth: []ssh.AuthMethod{
			ssh.PublicKeysCallback(agent.NewClient(agentConn).Signers),
		},
		HostKeyCallback: hkcb,
		Timeout:         30 * time.Second,
	}

	sc, err := ssh.Dial("tcp", host, &cfg)
	if err != nil {
		return nil, nil, err
	}

	sftpc, err := sftp.NewClient(sc)
	if err != nil {
		return nil, nil, err
	}

	return fs.NewSFTPFileSystem(sftpc, fs.Path(path)), func() error {
		if err := sftpc.Close(); err != nil {
			return err
		}
		if err := sc.Close(); err != nil {
			return err
		}
		return agentConn.Close()
	}, nil
}

func parseIgnoreFilter(lines string) (func(fs.Path) bool, error) {
	gi, err := ignore.CompileIgnoreLines(strings.Split(lines, "\n")...)
	if err != nil {
		return nil, err
	}

	return func(p fs.Path) bool {
		return gi.MatchesPath(string(p))
	}, nil
}
