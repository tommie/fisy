// Command fisy is the main entry point for users.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tommie/fisy/transfer"
)

func main() {
	// Make flag.Parsed() true so glog doesn't complain.
	flag.CommandLine.Parse(nil)

	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if eerr, ok := err.(*ExitError); ok {
			os.Exit(eerr.Code)
		}
		os.Exit(1)
	}
}

type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string {
	return e.Err.Error()
}

var (
	fileConc   int
	httpAddr   string
	ignoreSpec string
)

func init() {
	rootCmd.PersistentFlags().IntVar(&fileConc, "file-concurrency", 128, "number of files/directories to work on concurrently")
	rootCmd.PersistentFlags().StringVar(&httpAddr, "http-addr", "", "address to listen for HTTP requests on")
	rootCmd.PersistentFlags().StringVar(&ignoreSpec, "ignore", "", "filter to apply to ignore some files")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Set("--stderrthreshold", "WARNING")
}

//go:generate bash generate-version-go.sh
var rootCmd = cobra.Command{
	Use:     fmt.Sprintf("%s <source> <destination>", filepath.Base(os.Args[0])),
	Short:   "fisy - A bidirectional file synchronizer.",
	Version: programVersion,
	Args:    cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		if len(args) == 1 {
			return runAlias(args[0])
		}
		if len(args) != 2 {
			return fmt.Errorf("expected two arguments")
		}

		return runUpload(ctx, cmd, args[0], args[1])
	},
}

func runAlias(name string) error {
	var flags []string
	pflag.Visit(func(f *pflag.Flag) {
		flags = append(flags, "-"+f.Name+"="+f.Value.String())
	})
	executable, err := os.Executable()
	if err != nil {
		glog.Warningf("Couldn't get executable: %v", err)
		executable = os.Args[0]
	}
	path := os.ExpandEnv("$HOME/.config/fisy/") + name + ".alias"
	env := append([]string{"FISY=" + executable}, os.Environ()...)

	// First attempt to replace the process, to avoid glog writing a log here.
	syscall.Exec(path, append([]string{name + ".alias"}, flags...), env)

	// If that doesn't work: fork and exec.
	cmd := exec.Command(path, flags...)
	cmd.Env = env
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err == nil {
		return nil
	}
	if e, ok := err.(*exec.ExitError); ok {
		if ee, ok := e.Sys().(*syscall.WaitStatus); ok {
			return &ExitError{Code: ee.ExitStatus(), Err: e}
		}
	}

	return err
}

func runUpload(ctx context.Context, cmd *cobra.Command, srcSpec, destSpec string) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)

	if httpAddr != "" {
		ln, err := net.Listen("tcp", httpAddr)
		if err != nil {
			return err
		}

		go func() {
			defer ln.Close()

			server := &http.Server{Addr: ln.Addr().String(), Handler: nil}
			if err := server.Serve(ln); err != nil {
				glog.Exit(err)
			}
		}()
	}

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
