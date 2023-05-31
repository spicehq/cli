package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spiceai/gospice/v2"
	"github.com/spicehq/cli/pkg/arrow"
)

var (
	apiKeyFlag       string
	outputFormatFlag string
	showDetailsFlag  bool
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

		spice := gospice.NewSpiceClient()
		defer spice.Close()

		if err := spice.Init(apiKeyFlag); err != nil {
			cmd.PrintErrf("error intializing the Spice.xyz SDK: %s\n", err.Error())
			os.Exit(1)
		}

		startTime := time.Now()
		reader, err := spice.FireQuery(cmd.Context(), query)
		if err != nil {
			cmd.PrintErrf("error querying Spice.xyz: %s\n", err.Error())
			os.Exit(1)
		}
		defer reader.Release()
		queryExecutionTime := time.Since(startTime)

		for reader.Next() {
			record := reader.Record()
			defer record.Release()

			switch outputFormatFlag {
			case "csv":
				if err = arrow.WriteCsv(os.Stdout, record); err != nil {
					cmd.PrintErrf("error writing CSV: %s\n", err.Error())
					os.Exit(1)
				}
			case "json":
				data, err := record.MarshalJSON()
				if err != nil {
					cmd.PrintErrf("error writing JSON: %s\n", err.Error())
					os.Exit(1)
				}
				os.Stdout.Write(data)
				os.Stdout.WriteString("\n")
			default:
				cmd.PrintErrf("error: invalid output format: %s\n", outputFormatFlag)
				os.Exit(1)
			}
		}

		if showDetailsFlag {
			os.Stdout.WriteString("\n")
			cmd.Printf("Fetched in: %s\n", queryExecutionTime)
		}
	},
}

func init() {
	queryCmd.Flags().StringVar(&apiKeyFlag, "api_key", "", "Spice.xyz API Key")
	queryCmd.Flags().StringVar(&outputFormatFlag, "output_format", "csv", "Output format (csv or json)")
	queryCmd.Flags().BoolVar(&showDetailsFlag, "show_details", false, "Show details of query")

	RootCmd.AddCommand(queryCmd)
}
