package main

import (
	"github.com/pegasus-kv/pegasus-test-tools/tools/scheck"
	"github.com/spf13/cobra"
)

func runSCheckCommand(cmd *cobra.Command, args []string) {
	scheck.Run(globalContext)
}

func newSCheckCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "scheck",
		Short: "single cluster correctness checker",
		Run:   runSCheckCommand,
	}
}
