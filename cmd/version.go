package main

import (
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Spice.xyz CLI version",
	Example: `
spice version
`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Printf("Version: 0.1\n")
	},
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
