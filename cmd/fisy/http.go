package main

import (
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

var httpAddr string

func init() {
	rootCmd.PersistentFlags().StringVar(&httpAddr, "http-addr", "", "address to listen for HTTP requests on")

	cobra.OnInitialize(startHTTPServer)
}

var httpServerOnce sync.Once

func startHTTPServer() {
	if httpAddr == "" {
		return
	}

	httpServerOnce.Do(func() {
		ln, err := net.Listen("tcp", httpAddr)
		if err != nil {
			glog.Exitf("Failed to listen for HTTP server: %v", err)
		}

		go func() {
			defer ln.Close()

			server := &http.Server{Addr: ln.Addr().String(), Handler: nil}
			if err := server.Serve(ln); err != nil {
				glog.Exit(err)
			}
		}()
	})
}
