package toolbox

import (
	"github.com/pegasus-kv/pegasus-test-tools/tools/dcheck"
	"github.com/spf13/cobra"
)

func runDCheckCommand(cmd *cobra.Command, args []string) {
	dcheck.Run(globalContext)
}

func newDCheckCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "dcheck",
		Short: "duplication correctness checker",
		Run:   runDCheckCommand,
	}
}
