package main

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"github.com/tommie/fisy/fs"
	"github.com/tommie/fisy/remote"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// makeFileSystem creates a file system from a specification
// string. Returns the file system and a close function, or an
// error. The close function takes an error. In some file systems,
// passing non-nil will cause changes to be rolled back.
func makeFileSystem(s string) (fs.WriteableFileSystem, func(error) error, error) {
	u, err := parseFileSystemSpec(s)
	if err != nil {
		return nil, nil, err
	}
	return makeFileSystemFromURL(u)
}

// parseFileSystemSpec parses a string into a URL.
//
// Valid non-URLs shortcuts are:
//
//   <host>:<path>  - A remote SFTP location.
//   <path>         - A local location.
func parseFileSystemSpec(s string) (*url.URL, error) {
	if s == "" {
		return nil, fmt.Errorf("empty URL")
	}

	if strings.Contains(s, "://") {
		return url.Parse(s)
	}

	if strings.Contains(s, ":") {
		ss := strings.SplitN(s, ":", 2)
		return &url.URL{Scheme: "sftp", Host: ss[0], Path: ss[1]}, nil
	}

	return &url.URL{Scheme: "file", Path: s}, nil
}

// makeFileSystemFromURL creates a file system. Returns the file
// system and a close function, or an error. The close function takes
// an error. In some file systems, passing non-nil will cause changes
// to be rolled back.
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
		cfs, err := fs.NewCOW(raw, host, timeNow())
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
		return fs.NewLocal(u.Path), func(error) error { return nil }, nil

	case "sftp":
		host := u.Host
		if u.Port() == "" {
			host += defaultPortSuffix
		}

		user := os.Getenv("LOGNAME")
		if u.User != nil {
			if s := u.User.Username(); s != "" {
				user = s
			}
		}
		knownHostsPath := filepath.Join(os.Getenv("HOME"), ".ssh/known_hosts")
		if path := u.Query().Get("knownhosts"); path != "" {
			knownHostsPath = path
		}
		agentSockPath := os.Getenv("SSH_AUTH_SOCK")
		if path := u.Query().Get("authsock"); path != "" {
			agentSockPath = path
		}
		sftpc, err := remote.NewReconnectingSFTPClient(sftpClientDialler(host, user, knownHostsPath, agentSockPath))
		if err != nil {
			return nil, nil, err
		}
		return fs.NewSFTP(sftpc, fs.Path(u.Path)), func(error) error { return sftpc.Close() }, nil

	default:
		return nil, nil, fmt.Errorf("unknown URL scheme: %s", u.Scheme)
	}
}

var (
	// Note that ":ssh" doesn't work with the sftp library. It
	// would try to match a host key named "host:ssh" instead of
	// canonicalizing it to just "host".
	defaultPortSuffix = ":22"

	// timeNow is a mock injection point.
	timeNow = time.Now
)

// sftpClientDialler returns a dialler that can connect to the given host.
func sftpClientDialler(host, user, knownHostsPath, agentSockPath string) func() (remote.CloseableSFTPClient, error) {
	return func() (remote.CloseableSFTPClient, error) {
		hkcb, err := knownhosts.New(knownHostsPath)
		if err != nil {
			return nil, err
		}
		agentConn, err := net.Dial("unix", agentSockPath)
		if err != nil {
			return nil, err
		}
		cfg := ssh.ClientConfig{
			User: user,
			Auth: []ssh.AuthMethod{
				ssh.PublicKeysCallback(agent.NewClient(agentConn).Signers),
			},
			HostKeyCallback: hkcb,
			Timeout:         30 * time.Second,
		}

		sc, err := ssh.Dial("tcp", host, &cfg)
		if err != nil {
			return nil, err
		}

		sftpc, err := sftp.NewClient(sc)
		if err != nil {
			return nil, err
		}

		return &connectedSFTPClient{
			Client: sftpc,
			closers: []func() error{
				sc.Close,
				agentConn.Close,
			},
		}, nil
	}
}

// A connectSFTPClient is an SFTP client that can close multiple
// things. This is needed because sftp.Client doesn't necessarily
// close the ssh.Client and agent connection.
type connectedSFTPClient struct {
	*sftp.Client

	closers []func() error
}

// close runs Client.Close and then all the other closers.
func (c connectedSFTPClient) Close() error {
	if err := c.Client.Close(); err != nil {
		return err
	}
	for _, fun := range c.closers {
		if err := fun(); err != nil {
			return err
		}
	}
	return nil
}
