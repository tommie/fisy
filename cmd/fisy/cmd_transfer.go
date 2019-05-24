package main

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/tommie/fisy/transfer"
)

var (
	fileConc   int
	ignoreSpec string
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
	transferCmd.PersistentFlags().IntVar(&fileConc, "file-concurrency", 128, "number of files/directories to work on concurrently")
	transferCmd.PersistentFlags().StringVar(&ignoreSpec, "ignore", "", "filter to apply to ignore some files")

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

	start := time.Now()
	u := transfer.NewUpload(dest, src, transfer.WithIgnoreFilter(filter), transfer.WithConcurrency(fileConc))

	go RunProgress(ctx, u)

	if err := u.Run(ctx); err != nil {
		return err
	}
	cancel()

	stats := u.Stats()
	glog.Infof("All done in %v: %+v", time.Now().Sub(start), stats)
	fmt.Fprintf(
		cmd.OutOrStdout(),
		"All done in %v. Uploaded %v in %v file(s). Kept %v in %v file(s).\n",
		time.Now().Sub(start),
		storageBytes(stats.UploadedBytes), stats.UploadedFiles,
		storageBytes(stats.KeptBytes), stats.KeptFiles)

	return nil
}
