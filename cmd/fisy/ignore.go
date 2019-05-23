package main

import (
	"strings"

	"github.com/sabhiram/go-gitignore"
	"github.com/tommie/fisy/fs"
)

func parseIgnoreFilter(lines string) (func(fs.Path) bool, error) {
	gi, err := ignore.CompileIgnoreLines(strings.Split(lines, "\n")...)
	if err != nil {
		return nil, err
	}

	return func(p fs.Path) bool {
		return gi.MatchesPath(string(p))
	}, nil
}
