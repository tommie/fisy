package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func init() {
	ps, err := filepath.Glob(aliasPath("*"))
	if err != nil {
		return
	}

	for _, path := range ps {
		name := strings.TrimSuffix(filepath.Base(path), ".alias")
		rootCmd.AddCommand(&cobra.Command{
			Use:                name,
			Short:              "Alias found in " + path,
			RunE:               runAnAlias,
			DisableFlagParsing: true,
			SilenceUsage:       true,
		})
	}
}

func runAnAlias(c *cobra.Command, args []string) error {
	name := c.Name()

	var flags []string
	c.Flags().Visit(func(f *pflag.Flag) {
		flags = append(flags, "--"+f.Name+"="+f.Value.String())
	})
	flags = append(flags, args...)
	executable, err := os.Executable()
	if err != nil {
		executable = os.Args[0]
	}
	path := aliasPath(name)
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

func aliasPath(name string) string {
	return os.ExpandEnv("$HOME/.config/fisy/") + name + ".alias"
}
