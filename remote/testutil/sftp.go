package testutil

import (
	"io"
	"os"

	"github.com/pkg/sftp"
)

type pipe struct {
	io.ReadCloser
	io.WriteCloser
}

func (p *pipe) Close() error {
	p.ReadCloser.Close()
	p.WriteCloser.Close()
	return nil
}

// NewTestSFTPClient creates a new client that is connected to a
// server that operates on the host file system. Call the returned
// function to clean up after use.
func NewTestSFTPClient() (*sftp.Client, func(), error) {
	r1, w1, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	r2, w2, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}

	srv, err := sftp.NewServer(&pipe{r2, w1})
	if err != nil {
		return nil, nil, err
	}
	go srv.Serve()

	clnt, err := sftp.NewClientPipe(r1, w2)
	if err != nil {
		return nil, nil, err
	}

	return clnt, func() {
		// The reader given to client is the only stream not
		// closed by SFTP.
		r1.Close()
		clnt.Close()
		srv.Close()
	}, nil
}
