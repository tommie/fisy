package main

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"github.com/tommie/fisy/fs"
	"github.com/tommie/fisy/remote"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

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
		return &url.URL{Scheme: "sftp", Host: ss[0], Path: ss[1]}, nil
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
		return fs.NewLocal(u.Path), func(error) error { return nil }, nil

	case "sftp":
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
	dial := func() (remote.CloseableSFTPClient, error) {
		hkcb, err := knownhosts.New(os.ExpandEnv("$HOME/.ssh/known_hosts"))
		if err != nil {
			return nil, err
		}
		agentConn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
		if err != nil {
			return nil, err
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

	sftpc, err := remote.NewReconnectingSFTPClient(dial)
	if err != nil {
		return nil, nil, err
	}

	return fs.NewSFTPFileSystem(sftpc, fs.Path(path)), sftpc.Close, nil
}

type connectedSFTPClient struct {
	*sftp.Client

	closers []func() error
}

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
