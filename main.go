package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/logrusorgru/aurora"
	"github.com/spicehq/spar/pkg/spar"
	"github.com/valyala/fasthttp"
)

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Usage: spar <url>")
		fmt.Println()
		fmt.Println("Example: spar localhost:3000")
		return
	}

	uri := fasthttp.AcquireURI()
	defer fasthttp.ReleaseURI(uri)

	if err := uri.Parse(nil, []byte(flag.Arg(0))); err != nil {
		fmt.Println(err)
		return
	}

	if len(uri.Scheme()) == 0 {
		uri.SetScheme("http")
	}

	sparClient := spar.NewSparClient(uri)

	fmt.Printf("SPAR POST %s\n", aurora.BrightCyan(uri.String()))

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel,
		syscall.SIGINT)

	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for j := 0; j < 100; j++ {
				err := sparClient.Throw()
				if err != nil {
					log.Println(err.Error())
				}
			}
		}()
	}

	<-signalChannel

	sparClient.PrintStats()

	fmt.Println()
}
