package transfer

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestFilePairPDFS(t *testing.T) {
	ctx := context.Background()

	fun := func(ctx context.Context, fp *filePair) ([]*filePair, error) {
		if strings.Count(string(fp.path), "/") == 3 {
			return nil, nil
		}

		return []*filePair{
			&filePair{path: fp.path.Resolve("file1")},
			&filePair{path: fp.path.Resolve("file2")},
		}, nil
	}

	if err := filePairPDFS(ctx, []*filePair{&filePair{path: "root"}}, fun, 2); err != nil {
		t.Fatalf("filePairPDFS failed: %v", err)
	}
}

func TestFilePairPDFSHandlesEmpty(t *testing.T) {
	ctx := context.Background()

	if err := filePairPDFS(ctx, nil, nil, 1); err != nil {
		t.Fatalf("filePairPDFS failed: %v", err)
	}
}

func TestFilePairPDFSPropagatesError(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("mocked")

	fun := func(ctx context.Context, fp *filePair) ([]*filePair, error) {
		return nil, wantErr
	}

	if err := filePairPDFS(ctx, []*filePair{&filePair{path: "root"}}, fun, 2); err != wantErr {
		t.Fatalf("filePairPDFS: got %v, want %v", err, wantErr)
	}
}

func TestFilePairPDFSHandlesCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	fun := func(ctx context.Context, fp *filePair) ([]*filePair, error) {
		if strings.Count(string(fp.path), "/") == 1 {
			cancel()
			return nil, nil
		}

		return []*filePair{&filePair{path: fp.path.Resolve("file")}}, nil
	}

	if err := filePairPDFS(ctx, []*filePair{&filePair{path: "root"}}, fun, 2); err != context.Canceled {
		t.Fatalf("filePairPDFS failed: %v", err)
	}
}
