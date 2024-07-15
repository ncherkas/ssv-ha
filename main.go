package main

import (
	"context"
	"flag"
	"job/ssv-ha/duty"
	"job/ssv-ha/duty/ws"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	lateDutyWaitTime = 1 * time.Second
)

var endpoint = flag.String("endpoint", "ws://127.0.0.1:5000", "WS endpoint")
var dutyWaitTime = flag.Duration("dutyWaitTime", lateDutyWaitTime, "Late duties wait time")

func main() {
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)

	c, err := ws.NewClient(logger, *endpoint)
	if err != nil {
		logger.Panic(err)
	}
	defer c.Close()

	p := duty.NewProcessor(logger, c, *dutyWaitTime)
	logger.Println("Running SSV Home Assignement - the Duties Processor component...")
	if err = p.Run(ctx); err != nil {
		logger.Panic(err)
	}
}
