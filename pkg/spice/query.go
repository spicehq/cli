package spice

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/spiceai/gospice/v2"
	"github.com/spicehq/cli/pkg/arrow"
)

type Engine struct {
	spiceClient *gospice.SpiceClient
}

type QueryOptions struct {
	FireQuery    bool
	OutputFormat string
	ShowDetails  bool
}

func NewEngine(spiceClient *gospice.SpiceClient) *Engine {
	return &Engine{
		spiceClient: spiceClient,
	}
}

func (c *Engine) Query(ctx context.Context, sql string, options *QueryOptions) error {
	startTime := time.Now()
	var reader array.RecordReader
	var err error
	if options != nil && options.FireQuery {
		reader, err = c.spiceClient.FireQuery(ctx, sql)
	} else {
		reader, err = c.spiceClient.Query(ctx, sql)
	}
	if err != nil {
		return fmt.Errorf("error querying Spice.xyz: %w", err)
	}
	queryExecutionTime := time.Since(startTime)
	defer reader.Release()

	numRows := int64(0)

	for reader.Next() {
		record := reader.Record()
		numRows += record.NumRows()

		if options == nil || options.OutputFormat == "" || options.OutputFormat == "none" {
			continue
		}

		switch options.OutputFormat {
		case "csv":
			if err = arrow.WriteCsv(os.Stdout, record); err != nil {
				return fmt.Errorf("error writing CSV: %w", err)
			}
		case "json":
			data, err := record.MarshalJSON()
			if err != nil {
				return fmt.Errorf("error marshalling JSON: %w", err)
			}
			os.Stdout.Write(data)
			os.Stdout.WriteString("\n")
		}
	}

	if options.ShowDetails {
		os.Stdout.WriteString("\n")
		os.Stdout.WriteString(fmt.Sprintf("Fetched %d rows in: %s\n", numRows, queryExecutionTime))
	}

	return nil
}
