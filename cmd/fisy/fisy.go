// Command fisy is the main entry point for users.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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

func init() {
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Set("--stderrthreshold", "WARNING")
}

//go:generate bash generate-version-go.sh
var rootCmd = cobra.Command{
	Use:     fmt.Sprintf("%s <source> <destination>", filepath.Base(os.Args[0])),
	Short:   "fisy - A bidirectional file synchronizer.",
	Version: programVersion,
}
