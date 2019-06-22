package transfer

import (
	"os"

	"github.com/tommie/fisy/fs"
)

// A FileOperation describes one of the transfer file operations.
type FileOperation rune

const (
	UnknownFileOperation FileOperation = '?'
	Create               FileOperation = 'C'
	Remove               FileOperation = 'R'
	Keep                 FileOperation = 'K'
)

// commonModeMask is the non-special mode bits to transfer. Doesn't
// include file type bits.
const commonModeMask os.FileMode = 0xFFFFF

// A failPair describes a file in a transfer operation. The path
// identifies the file on both sides. src is nil if this is file has
// been removed, and dest is nil if the file didn't exist before.
type filePair struct {
	path fs.Path
	src  os.FileInfo
	dest os.FileInfo
}

// FileInfo returns overall file information about the file.
func (fp *filePair) FileInfo() os.FileInfo {
	if fp.src != nil {
		return fp.src
	}
	return fp.dest
}
