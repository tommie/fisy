package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/tommie/fisy/transfer"
	"github.com/tommie/fisy/transfer/terminal"
)

var (
	fileConc   int
	ignoreSpec string
	printOps   []string
)

var transferCmd = cobra.Command{
	Use:   "transfer <source> <destination>",
	Short: "Transfers files in one direction.",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		return runTransfer(ctx, cmd, args[0], args[1])
	},
}

func init() {
	transferCmd.PersistentFlags().IntVar(&fileConc, "file-concurrency", runtime.NumCPU()*32, "number of files/directories to work on concurrently")
	transferCmd.PersistentFlags().StringVar(&ignoreSpec, "ignore", "", "filter to apply to ignore some files")
	transferCmd.PersistentFlags().StringSliceVar(&printOps, "print-operations", nil, "types of file operations to print verbosely (a combination of create, update, keep, remove)")

	rootCmd.AddCommand(&transferCmd)
}

func runTransfer(ctx context.Context, cmd *cobra.Command, srcSpec, destSpec string) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)

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

	printOpsMap, err := parsePrintOps(printOps)
	if err != nil {
		return err
	}

	p := terminal.NewProgress(os.Stdout, 1*time.Second)
	u := transfer.NewUpload(
		dest, src,
		transfer.WithIgnoreFilter(filter),
		transfer.WithConcurrency(fileConc),
		transfer.WithFileHook(func(fi os.FileInfo, op transfer.FileOperation, err error) {
			if !printOpsMap[op] {
				return
			}
			p.FileHook(fi, op, err)
		}))

	go p.RunUpload(ctx, u)

	if err := u.Run(ctx); err != nil {
		cancel()
		return err
	}
	cancel()

	p.FinishUpload(u)

	return nil
}

// parsePrintOps parses the --print-operations flag into a set of FileOperation.
func parsePrintOps(ss []string) (map[transfer.FileOperation]bool, error) {
	ret := make(map[transfer.FileOperation]bool, len(ss))
	for _, s := range ss {
		switch s {
		case "create":
			ret[transfer.Create] = true
		case "remove":
			ret[transfer.Remove] = true
		case "keep":
			ret[transfer.Keep] = true
		case "update":
			ret[transfer.Update] = true
		default:
			return nil, fmt.Errorf("unknown file operation: %s", s)
		}
	}
	return ret, nil
}
