package main

import (
	"github.com/pegasus-kv/pegasus-test-tools/tools/scheck"
	"github.com/spf13/cobra"
)

var (
	withKillTest = false
)

func runSCheckCommand(cmd *cobra.Command, args []string) {
	scheck.Run(globalContext)
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
