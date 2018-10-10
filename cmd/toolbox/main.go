package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/op/go-logging"
	"github.com/spf13/cobra"
)

var (
	globalContext context.Context
	globalCancel  context.CancelFunc
)

var log = logging.MustGetLogger("main")

type pegasusLogger struct {
	plog *logging.Logger
}

func (p *pegasusLogger) Fatal(args ...interface{}) {
	p.plog.Panic(args...)
}

func (p *pegasusLogger) Fatalf(format string, args ...interface{}) {
	p.plog.Panicf(format, args...)
}

func (p *pegasusLogger) Fatalln(args ...interface{}) {
	p.plog.Panic(args...)
}

func (p *pegasusLogger) Print(args ...interface{}) {
	p.plog.Info(args...)
}

func (p *pegasusLogger) Printf(format string, args ...interface{}) {
	p.plog.Infof(format, args...)
}

func (p *pegasusLogger) Println(args ...interface{}) {
	p.plog.Info(args...)
}

func main() {
	runtime.GOMAXPROCS(2)

	filename := fmt.Sprintf("tlog-%s.txt", time.Now().Format("20060102150405"))
	lf, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
	if err != nil {
		panic(err)
	}
	beLogFile := logging.NewLogBackend(lf, "", 0)
	beStderr := logging.NewLogBackend(os.Stderr, "", 0)
	logging.SetBackend(beLogFile, beStderr)
	logging.SetFormatter(logging.MustStringFormatter("%{color}%{time:15:04:05.000} ▶ %{color:reset} %{message}"))
	pegalog.SetLogger(&pegasusLogger{
		plog: logging.MustGetLogger("pegasus"),
	})

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
		log.Infof("\nGot signal [%v] to exit.\n", sig)
		globalCancel()

		select {
		case <-sc:
			// send signal again, return directly
			log.Infof("\nGot signal [%v] again to exit.\n", sig)
			os.Exit(1)
		case <-time.After(10 * time.Second):
			log.Infof("\nWait 10s for closed, force exit\n")
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
	)

	cobra.EnablePrefixMatching = true

	rand.Seed(time.Now().UnixNano())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}
}
