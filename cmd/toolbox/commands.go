package main

import (
	"github.com/pegasus-kv/pegasus-test-tools/tools/dcheck"
	"github.com/pegasus-kv/pegasus-test-tools/tools/inject"
	"github.com/pegasus-kv/pegasus-test-tools/tools/scheck"
	"github.com/spf13/cobra"
)

func runDCheckCommand(cmd *cobra.Command, args []string) {
	dcheck.Run(globalContext)
}

func newDCheckCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "dcheck",
		Short: "duplication correctness checker",
		Run:   runDCheckCommand,
	}
	m.Flags().BoolVarP(&inject.WithKillTest, "kill", "k", false, "will randomly kill servers")
	m.Flags().BoolVarP(&inject.WithRollingUpdate, "roll", "r", false, "will run rolling update")
	return m
}

func runSCheckCommand(cmd *cobra.Command, args []string) {
	scheck.Run(globalContext)
}

func newSCheckCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "scheck",
		Short: "single cluster correctness checker",
		Run:   runSCheckCommand,
	}
	m.Flags().BoolVarP(&inject.WithKillTest, "kill", "k", false, "will randomly kill servers")
	m.Flags().BoolVarP(&inject.WithRollingUpdate, "roll", "r", false, "will run rolling update")
	return m
}
