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
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/tommie/fisy/transfer"
)

var (
	fileConc   = flag.Int("file-concurrency", 128, "number of files/directories to work on concurrently")
	httpAddr   = flag.String("http-addr", "", "address to listen for HTTP requests on")
	ignoreSpec = flag.String("ignore", "", "filter to apply to ignore some files")
)

func main() {
	ctx := context.Background()

	flag.Set("stderrthreshold", "WARNING")
	flag.Parse()

	if flag.NArg() == 1 {
		var flags []string
		flag.Visit(func(f *flag.Flag) {
			flags = append(flags, "-"+f.Name+"="+f.Value.String())
		})
		path := os.ExpandEnv("$HOME/.config/fisy/") + flag.Arg(0) + ".alias"
		env := append([]string{"FISY=" + os.Args[0]}, os.Environ()...)

		// First attempt to replace the process, to avoid glog writing a log here.
		syscall.Exec(path, append([]string{flag.Arg(0) + ".alias"}, flags...), env)

		// If that doesn't work: fork and exec.
		cmd := exec.Command(path, flags...)
		cmd.Env = env
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err == nil {
			return
		}
		if e, ok := err.(*exec.ExitError); ok {
			if ee, ok := e.Sys().(*syscall.WaitStatus); ok {
				os.Exit(ee.ExitStatus())
			}
		}

		glog.Error(err)
		os.Exit(1)
	}

	if flag.NArg() != 2 {
		glog.Error("expected two arguments")
		os.Exit(1)
	}

	if err := runUpload(ctx, flag.Arg(0), flag.Arg(1), *ignoreSpec); err != nil {
		glog.Error(err)
		os.Exit(10)
	}
}

func runUpload(ctx context.Context, srcSpec, destSpec, ignoreSpec string) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)

	if *httpAddr != "" {
		ln, err := net.Listen("tcp", *httpAddr)
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
	u := transfer.NewUpload(dest, src, transfer.WithIgnoreFilter(filter), transfer.WithConcurrency(*fileConc))

	go RunProgress(ctx, u)

	if err := u.Run(ctx); err != nil {
		return err
	}
	cancel()

	fmt.Println()

	glog.Infof("All done in %v: %+v", time.Now().Sub(start), u.Stats())

	return nil
}
