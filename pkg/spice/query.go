package spice

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/spiceai/gospice/v2"
	spice_arrow "github.com/spicehq/cli/pkg/arrow"
)

var (
	defaultWriterProps = []parquet.WriterProperty{
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithVersion(parquet.V1_0),
	}
	arrowprops = pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
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

	var recordsForParquet []arrow.Record
	for reader.Next() {
		record := reader.Record()
		numRows += record.NumRows()

		if options == nil || options.OutputFormat == "" || options.OutputFormat == "none" {
			continue
		}

		switch options.OutputFormat {
		case "csv":
			if err = spice_arrow.WriteCsv(os.Stdout, record); err != nil {
				return fmt.Errorf("error writing CSV: %w", err)
			}
		case "json":
			data, err := record.MarshalJSON()
			if err != nil {
				return fmt.Errorf("error marshalling JSON: %w", err)
			}
			os.Stdout.Write(data)
			os.Stdout.WriteString("\n")
		case "parquet":
			record.Retain()
			recordsForParquet = append(recordsForParquet, record)
		}
	}

	if len(recordsForParquet) > 0 {
		defer func() {
			for _, record := range recordsForParquet {
				record.Release()
			}
		}()
		schema := recordsForParquet[0].Schema()
		parquetFileName := fmt.Sprintf("spice-%d.parquet", time.Now().UnixNano())
		parquetFile, err := os.Create(parquetFileName)
		if err != nil {
			return fmt.Errorf("error creating parquet file: %w", err)
		}
		parquetWriter, err := pqarrow.NewFileWriter(schema, parquetFile, parquet.NewWriterProperties(defaultWriterProps...), arrowprops)
		if err != nil {
			return fmt.Errorf("error creating parquet writer: %w", err)
		}

		for _, record := range recordsForParquet {
			err = parquetWriter.WriteBuffered(record)
			if err != nil {
				parquetWriter.Close()
				return fmt.Errorf("error writing parquet file: %w", err)
			}
		}
		parquetWriter.Close()
		os.Stdout.WriteString(fmt.Sprintf("Wrote %d rows to %s\n", numRows, parquetFileName))
	}

	if options.ShowDetails {
		os.Stdout.WriteString(fmt.Sprintf("Fetched %d rows in: %s\n", numRows, queryExecutionTime))
	}

	return nil
}
