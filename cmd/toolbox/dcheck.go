package main

import (
	"github.com/pegasus-kv/pegasus-test-tools/tools/dcheck"
	"github.com/spf13/cobra"
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
