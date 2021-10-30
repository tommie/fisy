package build

import (
	"fmt"
)

// Injected with -ldflags.
var (
	buildSource   string
	buildBranch   string
	buildRevision string
	buildIsClean  string
	buildDate     string
)

var version string

func init() {
	if buildDate == "" || buildRevision == "" || buildIsClean == "" {
		panic("missing build information. Not using make?")
	}

	var dirty string
	if buildIsClean != "true" {
		dirty = "dirty"
	}
	version = fmt.Sprintf("%s %s %s", buildDate, buildRevision, dirty)
}

func VersionString() string {
	return version
}
