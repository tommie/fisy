package main

import (
	"strings"

	"github.com/sabhiram/go-gitignore"
	"github.com/tommie/fisy/fs"
)

func parseIgnoreFilter(lines string) (func(fs.Path) bool, error) {
	gi := ignore.CompileIgnoreLines(strings.Split(lines, "\n")...)

	return func(p fs.Path) bool {
		return gi.MatchesPath(string(p))
	}, nil
}
