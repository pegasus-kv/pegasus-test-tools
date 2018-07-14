package main

import (
	"fmt"
	"github.com/pegasus-kv/pegasus-test-tools/tools/bbench"
	"github.com/pegasus-kv/pegasus-test-tools/tools/dcheck"
	"github.com/pegasus-kv/pegasus-test-tools/tools/scheck"
	"github.com/spf13/cobra"
)

var (
	withKillTest bool
)

func runDCheckCommand(cmd *cobra.Command, args []string) {
	dcheck.Run(globalContext, withKillTest)
}

func newDCheckCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "dcheck",
		Short: "duplication correctness checker",
		Run:   runDCheckCommand,
	}
	m.Flags().BoolVarP(&withKillTest, "kill", "k", false, "will randomly kill servers")
	return m
}

func runSCheckCommand(cmd *cobra.Command, args []string) {
	scheck.Run(globalContext, withKillTest)
}

func newSCheckCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "scheck",
		Short: "single cluster correctness checker",
		Run:   runSCheckCommand,
	}
	m.Flags().BoolVarP(&withKillTest, "kill", "k", false, "will randomly kill servers")
	return m
}

func runBBenchCommand(cmd *cobra.Command, args []string) {
	if len(args) != 1 && (args[0] != "load" && args[0] != "run") {
		fmt.Println("invalid argument: ", args)
	}
	bbench.Run(globalContext, args[0])
}

func newBBenchCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "bbench",
		Short: "benchmark of batch operations",
		Run:   runBBenchCommand,
	}
	return m
}
