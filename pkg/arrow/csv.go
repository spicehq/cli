package arrow

import (
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/csv"
)

func WriteCsv(f io.Writer, record arrow.Record) error {
	w := csv.NewWriter(f, record.Schema(), csv.WithHeader(true), csv.WithComma(','))
	err := w.Write(record)
	if err != nil {
		return err
	}

	err = w.Flush()
	if err != nil {
		return err
	}

	err = w.Error()
	if err != nil {
		return err
	}

	return nil
}
