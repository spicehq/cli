package main

import (
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

var schemasCmd = &cobra.Command{
	Use:   "schemas",
	Short: "Spice.xyz CLI schemas",
	Example: `
spice schemas
`,
	Run: func(cmd *cobra.Command, args []string) {
		if apiKeyFlag == "" {
			cmd.PrintErrf("error: --api_key is required\n")
			os.Exit(1)
		}

		resp, err := http.DefaultClient.Get("https://dev-data.spiceai.io/v0.1/schemas")
		if err != nil {
			cmd.PrintErrf("error: %s\n", err.Error())
			os.Exit(1)
		}

		if resp.StatusCode != 200 {
			cmd.PrintErrf("error: %s\n", resp.Status)
			os.Exit(1)
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			cmd.PrintErrf("error: %s\n", err.Error())
			os.Exit(1)
		}

		os.Stdout.Write(data)
	},
}

func init() {
	schemasCmd.Flags().StringVar(&apiKeyFlag, "api_key", "", "Spice.xyz API Key")

	RootCmd.AddCommand(schemasCmd)
}
