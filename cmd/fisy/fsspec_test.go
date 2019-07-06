package main

import (
	"crypto/rsa"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/tommie/fisy/fs"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
	"golang.org/x/sync/errgroup"
)

func TestMakeFileSystem(t *testing.T) {
	wfs, done, err := makeFileSystem("/tmp")
	if err != nil {
		t.Fatalf("makeFileSystem failed: %v", err)
	}
	defer done(nil)

	if _, ok := wfs.(*fs.Local); !ok {
		t.Errorf("makeFileSystem: got %T, want *fs.Local", wfs)
	}
}

func TestParseFileSystemSpec(t *testing.T) {
	tsts := []struct {
		S       string
		Want    url.URL
		WantErr error
	}{
		{"", url.URL{}, fmt.Errorf("empty URL")},
		{"abc", url.URL{Scheme: "file", Path: "abc"}, nil},
		{"/abc", url.URL{Scheme: "file", Path: "/abc"}, nil},
		{"file:///abc", url.URL{Scheme: "file", Path: "/abc"}, nil},
		{"host:/abc", url.URL{Scheme: "sftp", Host: "host", Path: "/abc"}, nil},
	}
	for _, tst := range tsts {
		t.Run(tst.S, func(t *testing.T) {
			got, err := parseFileSystemSpec(tst.S)
			if !reflect.DeepEqual(err, tst.WantErr) {
				t.Fatalf("parseFileSystemSpec error: got %#v, want %#v", err, tst.WantErr)
			}
			if err == nil && !reflect.DeepEqual(got, &tst.Want) {
				t.Errorf("parseFileSystemSpec: got %+v, want %+v", got, tst.Want)
			}
		})
	}
}

func TestMakeFileSystemFromURL(t *testing.T) {
	timeNow = func() time.Time { return time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC) }
	defer func() {
		timeNow = time.Now
	}()

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("os.Hostname failed: %v", err)
	}

	newCOW := func(raw fs.WriteableFileSystem, host string, t time.Time) *fs.COW {
		cow, err := fs.NewCOW(raw, host, t)
		if err != nil {
			panic(err)
		}
		return cow
	}

	tmpd, err := ioutil.TempDir("", "fsspec-test-")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(tmpd)

	// For COW, we create the host's directory.
	os.Mkdir(filepath.Join(tmpd, hostname), 0700)

	tsts := []struct {
		URL     url.URL
		Want    fs.WriteableFileSystem
		WantErr error
	}{
		{url.URL{}, nil, fmt.Errorf("unknown URL scheme: ")},
		{url.URL{Scheme: "file", Path: tmpd}, fs.NewLocal(tmpd), nil},
		{url.URL{Scheme: "cow+file", Path: tmpd}, newCOW(fs.NewLocal(tmpd), hostname, timeNow()), nil},
	}
	for _, tst := range tsts {
		t.Run(tst.URL.String(), func(t *testing.T) {
			wfs, done, err := makeFileSystemFromURL(&tst.URL)
			if !reflect.DeepEqual(err, tst.WantErr) {
				t.Fatalf("makeFileSystem error: got %#v, want %#v", err, tst.WantErr)
			}
			if err != nil {
				return
			}

			if !reflect.DeepEqual(wfs, tst.Want) {
				t.Errorf("makeFileSystem: got %#v, want %#v", wfs, tst.Want)
			}

			if err := done(nil); err != nil {
				t.Errorf("done failed: %v", err)
			}
		})
	}

	t.Run("sftp", func(t *testing.T) {
		sshAddr, agentPath, knownHostsPath, done, err := newTestSFTPServer(tmpd)
		if err != nil {
			t.Fatalf("newTestSFTPServer failed: %v", err)
		}
		defer done()

		defaultPortSuffix = fmt.Sprintf(":%d", sshAddr.Port)
		defer func() {
			defaultPortSuffix = ":22"
		}()

		wfs, close, err := makeFileSystemFromURL(&url.URL{
			Scheme: "sftp",
			// We don't use the port here, so we exercise the defaultPortSuffix code path.
			Host: fmt.Sprintf("[%s]", sshAddr.IP.String()),
			User: url.User("tester"),
			Path: tmpd,
			RawQuery: url.Values{
				"authsock":   []string{agentPath},
				"knownhosts": []string{knownHostsPath},
			}.Encode(),
		})
		if err != nil {
			t.Fatalf("makeFileSystemFromURL failed: %v", err)
		}

		if _, ok := wfs.(*fs.SFTP); !ok {
			t.Errorf("makeFileSystemFromURL: got %T, want *fs.SFTP", wfs)
		}

		if err := close(nil); err != nil {
			t.Errorf("close failed: %v", err)
		}
	})
}

func newTestSFTPServer(tmpd string) (*net.TCPAddr, string, string, func() error, error) {
	var closed uint32
	var eg errgroup.Group
	var closers []func() error
	done := func() error {
		atomic.StoreUint32(&closed, 1)
		for i := len(closers); i > 0; i-- {
			closers[i-1]()
		}
		return eg.Wait()
	}

	agentPath := filepath.Join(tmpd, "agent.sock")
	al, err := net.ListenUnix("unix", &net.UnixAddr{Name: agentPath})
	if err != nil {
		return nil, "", "", nil, err
	}
	closers = append(closers, al.Close)

	eg.Go(func() error {
		for {
			conn, err := al.Accept()
			if err != nil {
				if atomic.LoadUint32(&closed) != 0 {
					return nil
				}
				return err
			}
			eg.Go(func() error {
				defer conn.Close()

				if err := agent.ServeAgent(agent.NewKeyring(), conn); err != nil && err != io.EOF {
					return err
				}
				return nil
			})
		}
	})

	sl, err := net.ListenTCP("tcp", &net.TCPAddr{})
	if err != nil {
		done()
		return nil, "", "", nil, err
	}
	closers = append(closers, sl.Close)

	sconfig := &ssh.ServerConfig{
		NoClientAuth: true,
	}
	rk, err := rsa.GenerateKey(rand.New(rand.NewSource(0)), 1024)
	if err != nil {
		done()
		return nil, "", "", nil, err
	}
	hk, err := ssh.NewSignerFromKey(rk)
	if err != nil {
		done()
		return nil, "", "", nil, err
	}
	sconfig.AddHostKey(hk)
	knownHostsPath := filepath.Join(tmpd, "known_hosts")
	khf, err := os.Create(knownHostsPath)
	if err != nil {
		done()
		return nil, "", "", nil, err
	}
	khf.WriteString(knownhosts.Line([]string{sl.Addr().String()}, hk.PublicKey()))
	if err := khf.Close(); err != nil {
		done()
		return nil, "", "", nil, err
	}

	eg.Go(func() error {
		for {
			conn, err := sl.Accept()
			if err != nil {
				if atomic.LoadUint32(&closed) != 0 {
					return nil
				}
				return err
			}
			sconn, nchans, reqs, err := ssh.NewServerConn(conn, sconfig)
			if err != nil {
				return err
			}
			eg.Go(func() error {
				ssh.DiscardRequests(reqs)
				return nil
			})
			eg.Go(func() error {
				defer sconn.Close()
				for nchan := range nchans {
					if nchan.ChannelType() != "session" {
						nchan.Reject(ssh.UnknownChannelType, "unhandled channel type")
						continue
					}
					ch, reqs, err := nchan.Accept()
					if err != nil {
						return err
					}
					s, err := sftp.NewServer(ch)
					if err != nil {
						return err
					}

					eg.Go(func() error {
						for req := range reqs {
							if req.WantReply {
								req.Reply(req.Type == "subsystem", nil)
							}
						}
						return nil
					})
					eg.Go(func() error {
						defer ch.Close()

						if err := s.Serve(); err != nil && err != io.EOF {
							return err
						}
						return nil
					})
				}
				return nil
			})
		}
	})

	return sl.Addr().(*net.TCPAddr), agentPath, knownHostsPath, done, nil
}
