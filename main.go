package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/spicehq/spar/pkg/spar"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Usage: spar <throws> <sql>")
		fmt.Println()
		fmt.Println(`Example: spar "SELECT * FROM eth.recent_blocks ORDER BY number DESC LIMIT 10"`)
		return
	}

	throws, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		log.Fatalf("invalid throws: %s", err.Error())
	}
	sql := flag.Arg(1)

	sparClient := spar.NewSparClient()
	if err := sparClient.Init(); err != nil {
		log.Fatalf("failed to initialize spar client: %s", err.Error())
	}
	defer sparClient.Close()

	log.Printf("throwing %d times using %d CPUs.\n", throws, runtime.NumCPU())

	errGroup, errGroupCtx := errgroup.WithContext(ctx)
	errGroup.SetLimit(runtime.NumCPU())

	startTime := time.Now()
	for j := 0; j < throws; j++ {
		errGroup.Go(func() error {
			return sparClient.Throw(errGroupCtx, sql)
		})
	}

	if err := errGroup.Wait(); err != nil {
		log.Fatalf("failed to throw: %s", err.Error())
	}

	duration := time.Since(startTime)

	fmt.Printf("Done in %s\n\n", duration.Round(time.Microsecond))

	sparClient.PrintStats()

	fmt.Println()
}
