module github.com/tommie/fisy

go 1.16

require (
	github.com/golang/glog v1.0.0
	github.com/pkg/sftp v1.13.4
	github.com/sabhiram/go-gitignore v0.0.0-20210923224102-525f6e181f06
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

require (
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/pkg/errors v0.8.1 // indirect
	golang.org/x/sys v0.0.0-20210615035016-665e8c7367d1 // indirect
	golang.org/x/term v0.0.0-20201126162022-7de9c90e9dd1 // indirect
)

replace github.com/pkg/sftp => ./vendored/github.com/pkg/sftp
