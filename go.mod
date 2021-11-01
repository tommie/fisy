module github.com/tommie/fisy

go 1.16

//xgo:imports locals: github.com/tommie/fisy

require (
	github.com/golang/glog v1.0.0
	github.com/pkg/sftp v1.13.5-0.20211030161311-7adab6cb02e2
	github.com/sabhiram/go-gitignore v0.0.0-20210923224102-525f6e181f06
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

require github.com/vbauerster/mpb/v7 v7.1.5
