//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"golang.org/x/sync/errgroup"
)

const (
	maxWorkerCount       = 10
	maxMutationBatchSize = 100
)

type Column struct {
	Name     string
	Type     sppb.TypeCode
	Array    bool
	Nullable bool
}

type Copier struct {
	Table    string
	Columns  []Column
	FilePath string
	Header   bool
}

type CopyStats struct {
	Rows              int
	MutationBatchSize int
	WorkerCount       int
}

func NewCopier(table string, columns []Column, filePath string, header bool) *Copier {
	return &Copier{Table: table, Columns: columns, FilePath: filePath, Header: header}
}

func (c *Copier) Copy(ctx context.Context, session *Session) (*CopyStats, error) {
	stats, err := c.getCopyStats()
	if err != nil {
		return nil, err
	}
	// No-op
	if stats.Rows == 0 {
		return stats, nil
	}

	recordChannel := make(chan []string)

	// Start processing workers
	errgroup, groupctx := c.startWorkers(ctx, session.client, stats, recordChannel)

	// Start a reader
	c.startReader(errgroup, groupctx, recordChannel)

	if err = errgroup.Wait(); err != nil {
		return nil, err
	}

	return stats, err
}

func (c *Copier) getCopyStats() (*CopyStats, error) {
	// Open the CSV file
	f, err := os.Open(c.FilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Create a stats CSV reader
	r := csv.NewReader(f)
	// Reusing memory since only collecting stats
	r.ReuseRecord = true

	// Read the CSV file line by line, validate and compute row count. Ignore the header.
	rows := 0
	if c.Header {
		rows = -1
	}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows++

		if len(record) != len(c.Columns) {
			return nil, fmt.Errorf("illegal number of columns. Expected %d, received %d", len(c.Columns), len(record))
		}

		for i, val := range record {
			// Evaluate the header if specified
			if rows == 0 && !strings.EqualFold(c.Columns[i].Name, val) {
				return nil, fmt.Errorf("column `%s` didn't match expected column `%s`", val, c.Columns[i].Name)
			} else if !c.Columns[i].Nullable && len(val) == 0 {
				return nil, fmt.Errorf("cannot use null value for non-nullable column `%s`", c.Columns[i].Name)
			}
		}
	}

	// Compute worker pool size and mutation batch size
	// TODO compute limits based on max num mutations/max tx size
	workerCount := rows / maxMutationBatchSize
	if rows%maxMutationBatchSize > 0 {
		workerCount++
	}
	mutationBatchSize := maxMutationBatchSize
	if rows < maxMutationBatchSize {
		mutationBatchSize = rows
	}
	return &CopyStats{Rows: rows, WorkerCount: workerCount, MutationBatchSize: mutationBatchSize}, nil
}

func (c *Copier) startWorkers(parentCtx context.Context, client *spanner.Client, stats *CopyStats, recordChannel chan []string) (*errgroup.Group, context.Context) {
	// TODO adjust the session count and connection number based on worker count. Current max worker count == max session count.
	errs, ctx := errgroup.WithContext(parentCtx)

	// Prepare col names
	cols := make([]string, len(c.Columns))
	for i, col := range c.Columns {
		cols[i] = col.Name
	}

	// Start workers
	for i := 0; i < stats.WorkerCount; i++ {
		errs.Go(func() error {
			batch := make([]*spanner.Mutation, stats.MutationBatchSize)
			numProcessed := 0
			for record := range recordChannel {
				// Cancel on first error.
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				vals := make([]any, len(c.Columns))
				for i, field := range record {
					val, err := encodeValue(field, c.Columns[i].Type)
					if err != nil {
						return fmt.Errorf("failed to encode value %s to type %v: %w", field, c.Columns[i].Type, err)
					}
					vals[i] = val
				}

				batch[numProcessed%stats.MutationBatchSize] = spanner.InsertOrUpdate(c.Table, cols, vals)
				numProcessed++

				if numProcessed%stats.MutationBatchSize == 0 {
					if _, err := client.Apply(ctx, batch); err != nil {
						return err
					}
				}
			}
			if numProcessed > 0 && numProcessed%stats.MutationBatchSize > 0 {
				if _, err := client.Apply(ctx, batch[:numProcessed%stats.MutationBatchSize]); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return errs, ctx
}

func (c *Copier) startReader(errs *errgroup.Group, ctx context.Context, recordChannel chan []string) {
	errs.Go(func() error {
		// Defer close channel
		defer close(recordChannel)

		// Open the CSV file
		f, err := os.Open(c.FilePath)
		if err != nil {
			return err
		}
		defer f.Close()

		// Create a processing CSV reader
		r := csv.NewReader(f)

		// Read the CSV file line by line
		header := c.Header
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if header {
				header = false
				continue
			}

			select {
			// Cancel on first error.
			case <-ctx.Done():
				return ctx.Err()
			// Enqueue record for processing
			case recordChannel <- record:
			}
		}
		return nil
	})
}

func encodeValue(val string, typeCode sppb.TypeCode) (any, error) {
	// Allowable types: https://cloud.google.com/spanner/docs/data-types#allowable-types
	switch typeCode {
	case sppb.TypeCode_BOOL:
		var v spanner.NullBool
		if err := v.UnmarshalJSON(nullOrBytes(val)); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_BYTES:
		if len(val) == 0 {
			return []byte(nil), nil
		}
		return []byte(val), nil
	case sppb.TypeCode_FLOAT64:
		var v spanner.NullFloat64
		if err := v.UnmarshalJSON(nullOrBytes(val)); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_INT64:
		var v spanner.NullInt64
		if err := v.UnmarshalJSON(nullOrBytes(val)); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_NUMERIC:
		var v spanner.NullNumeric
		if err := v.UnmarshalJSON(nullOrQuotedBytes(val)); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_STRING:
		var v spanner.NullString
		v.StringVal = val
		v.Valid = len(val) != 0
		return v, nil
	case sppb.TypeCode_TIMESTAMP:
		var v spanner.NullTime
		if err := v.UnmarshalJSON(nullOrQuotedBytes(val)); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_DATE:
		var v spanner.NullDate
		if err := v.UnmarshalJSON(nullOrQuotedBytes(val)); err != nil {
			return nil, err
		}
		return v, nil
	case sppb.TypeCode_JSON:
		var v spanner.NullJSON
		if err := v.UnmarshalJSON(nullOrBytes(val)); err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", typeCode.String())
	}
}

func nullOrBytes(val string) []byte {
	if len(val) == 0 {
		return []byte("null")
	}
	return []byte(val)
}

func nullOrQuotedBytes(val string) []byte {
	if len(val) == 0 {
		return []byte("null")
	}
	return []byte(fmt.Sprintf("\"%s\"", val))
}
