package main

import (
	"github.com/pegasus-kv/pegasus-test-tools/tools/dcheck"
	"github.com/pegasus-kv/pegasus-test-tools/tools/scheck"
	"github.com/spf13/cobra"
)

var (
	withKillTest      bool
	withRollingUpdate bool
)

func runDCheckCommand(cmd *cobra.Command, args []string) {
	dcheck.Run(globalContext, withKillTest, withRollingUpdate)
}

func newDCheckCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "dcheck",
		Short: "duplication correctness checker",
		Run:   runDCheckCommand,
	}
	m.Flags().BoolVarP(&withKillTest, "kill", "k", false, "will randomly kill servers")
	m.Flags().BoolVarP(&withRollingUpdate, "roll", "r", false, "will run rolling update")
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
