package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/gospice/v2"
	"github.com/spicehq/cli/pkg/spice"
)

var (
	apiKeyFlag       string
	outputFormatFlag string
	showDetailsFlag  bool
	iterations       int
	fireQuery        bool
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Spice.xyz CLI query",
	Example: `
spice query
`,
	Run: func(cmd *cobra.Command, args []string) {
		if apiKeyFlag == "" {
			cmd.PrintErrf("error: --api_key is required\n")
			os.Exit(1)
		}

		var query = "Select number, hash FROM eth.recent_blocks ORDER BY number DESC LIMIT 3"
		if len(args) > 0 {
			query = args[0]
		}

		spiceClient := gospice.NewSpiceClient()
		defer spiceClient.Close()

		if err := spiceClient.Init(apiKeyFlag); err != nil {
			cmd.PrintErrf("error intializing the Spice.xyz SDK: %s\n", err.Error())
			os.Exit(1)
		}

		engine := spice.NewEngine(spiceClient)

		for i := 0; i < iterations; i++ {
			if err := engine.Query(cmd.Context(), query, &spice.QueryOptions{
				FireQuery:    fireQuery,
				OutputFormat: outputFormatFlag,
				ShowDetails:  showDetailsFlag,
			}); err != nil {
				cmd.PrintErrf("error querying Spice.xyz: %s\n", err.Error())
				os.Exit(1)
			}
		}
	},
}

func init() {
	queryCmd.Flags().StringVar(&apiKeyFlag, "api_key", "", "Spice.xyz API Key")
	queryCmd.Flags().StringVar(&outputFormatFlag, "output_format", "csv", "Output format (csv, json, or none)")
	queryCmd.Flags().BoolVar(&showDetailsFlag, "show_details", false, "Show details of query")
	queryCmd.Flags().BoolVar(&fireQuery, "firequery", false, "Fire query")
	queryCmd.Flags().IntVar(&iterations, "iterations", 1, "Number of iterations to run the query")

	RootCmd.AddCommand(queryCmd)
}
