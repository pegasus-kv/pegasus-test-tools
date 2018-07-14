package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var (
	globalContext context.Context
	globalCancel  context.CancelFunc
)

func main() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	globalContext, globalCancel = context.WithCancel(context.Background())

	closeDone := make(chan struct{}, 1)
	go func() {
		sig := <-sc
		log.Printf("\nGot signal [%v] to exit.\n", sig)
		globalCancel()

		select {
		case <-sc:
			// send signal again, return directly
			log.Printf("\nGot signal [%v] again to exit.\n", sig)
			os.Exit(1)
		case <-time.After(10 * time.Second):
			log.Printf("\nWait 10s for closed, force exit\n")
			os.Exit(1)
		case <-closeDone:
			return
		}
	}()

	rootCmd := &cobra.Command{
		Use:   "toolbox",
		Short: "Pegasus test tools",
	}

	rootCmd.AddCommand(
		newSCheckCommand(),
		newDCheckCommand(),
		newBBenchCommand(),
	)

	cobra.EnablePrefixMatching = true

	rand.Seed(time.Now().UnixNano())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}
}
