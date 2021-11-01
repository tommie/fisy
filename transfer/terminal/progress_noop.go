package terminal

import (
	"context"
	"os"

	"github.com/tommie/fisy/transfer"
)

// A NoOpProgress doesn't report progress to a terminal.
type NoOpProgress struct{}

func (NoOpProgress) FileHook(os.FileInfo, transfer.FileOperation, *uint64, error) {}
func (NoOpProgress) RunUpload(context.Context, Upload)                            {}
func (NoOpProgress) FinishUpload(Upload)                                          {}
