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
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	pb "cloud.google.com/go/spanner/apiv1/spannerpb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"cloud.google.com/go/spanner/spansql"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func isEqual(got, want any) bool {
	switch got.(type) {
	case []byte:
		left, okLeft := got.([]byte)
		right, okRight := want.([]byte)
		if !okLeft || !okRight {
			return false
		}
		return bytes.Equal(left, right)
	case spanner.NullNumeric:
		left, okLeft := got.(spanner.NullNumeric)
		right, okRight := want.(spanner.NullNumeric)
		if !okLeft || !okRight {
			return false
		}
		return left.String() == right.String()
	case spanner.NullTime:
		left, okLeft := got.(spanner.NullTime)
		right, okRight := want.(spanner.NullTime)
		if !okLeft || !okRight {
			return false
		}
		return left.String() == right.String()
	case spanner.NullDate:
		left, okLeft := got.(spanner.NullDate)
		right, okRight := want.(spanner.NullDate)
		if !okLeft || !okRight {
			return false
		}
		return left.String() == right.String()
	case spanner.NullJSON:
		left, leftErr := json.Marshal(got)
		right, rightErr := json.Marshal(got)
		if leftErr != nil || rightErr != nil {
			return false
		}
		return bytes.Equal(left, right)
	default:
		return got == want
	}
}

func TestEncodeValue(t *testing.T) {
	tests := []struct {
		desc     string
		value    string
		typeCode sppb.TypeCode
		want     any
	}{
		// non-nullable
		{
			desc:     "bool",
			value:    "true",
			typeCode: sppb.TypeCode_BOOL,
			want:     spanner.NullBool{Bool: true, Valid: true},
		},
		{
			desc:     "bytes",
			value:    "abcd",
			typeCode: sppb.TypeCode_BYTES,
			want:     []byte{'a', 'b', 'c', 'd'},
		},
		{
			desc:     "float64",
			value:    "1.230000",
			typeCode: sppb.TypeCode_FLOAT64,
			want:     spanner.NullFloat64{Float64: 1.23, Valid: true},
		},
		{
			desc:     "int64",
			value:    "123",
			typeCode: sppb.TypeCode_INT64,
			want:     spanner.NullInt64{Int64: 123, Valid: true},
		},
		{
			desc:     "numeric",
			value:    "1.23",
			typeCode: sppb.TypeCode_NUMERIC,
			want:     spanner.NullNumeric{Numeric: *big.NewRat(123, 100), Valid: true},
		},
		{
			desc:     "string",
			value:    "foo",
			typeCode: sppb.TypeCode_STRING,
			want:     spanner.NullString{StringVal: "foo", Valid: true},
		},
		{
			desc:     "timestamp",
			value:    "2018-01-23T03:00:00Z",
			typeCode: sppb.TypeCode_TIMESTAMP,
			want:     spanner.NullTime{Time: time.Unix(1516676400, 0).UTC(), Valid: true},
		},
		{
			desc:     "date",
			value:    "2018-01-23",
			typeCode: sppb.TypeCode_DATE,
			want:     spanner.NullDate{Date: civil.DateOf(time.Unix(1516676400, 0).UTC()), Valid: true},
		},
		{
			desc:     "json",
			value:    `{"msg":"foo"}`,
			typeCode: sppb.TypeCode_JSON,
			want:     spanner.NullJSON{Value: jsonMessage{Msg: "foo"}, Valid: true},
		},
		{
			desc:     "json null is not NULL",
			value:    `null`,
			typeCode: sppb.TypeCode_JSON,
			want:     spanner.NullJSON{Value: nil, Valid: true},
		},

		// nullable
		{
			desc:     "null bool",
			value:    "",
			typeCode: sppb.TypeCode_BOOL,
			want:     spanner.NullBool{Bool: false, Valid: false},
		},
		{
			desc:     "null bytes",
			value:    "",
			typeCode: sppb.TypeCode_BYTES,
			want:     []byte(nil),
		},
		{
			desc:     "null float64",
			value:    "",
			typeCode: sppb.TypeCode_FLOAT64,
			want:     spanner.NullFloat64{Float64: 0, Valid: false},
		},
		{
			desc:     "null int64",
			value:    "",
			typeCode: sppb.TypeCode_INT64,
			want:     spanner.NullInt64{Int64: 0, Valid: false},
		},
		{
			desc:     "null numeric",
			value:    "",
			typeCode: sppb.TypeCode_NUMERIC,
			want:     spanner.NullNumeric{Numeric: big.Rat{}, Valid: false},
		},
		{
			desc:     "null string",
			value:    "",
			typeCode: sppb.TypeCode_STRING,
			want:     spanner.NullString{StringVal: "", Valid: false},
		},
		{
			desc:     "null time",
			value:    "",
			typeCode: sppb.TypeCode_TIMESTAMP,
			want:     spanner.NullTime{Time: time.Unix(0, 0).UTC(), Valid: false},
		},
		{
			desc:     "null date",
			value:    "",
			typeCode: sppb.TypeCode_DATE,
			want:     spanner.NullDate{Date: civil.DateOf(time.Unix(0, 0).UTC()), Valid: false},
		},
		{
			desc:     "null json",
			value:    "",
			typeCode: sppb.TypeCode_JSON,
			want:     spanner.NullJSON{Value: nil, Valid: false},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			got, err := encodeValue(test.value, test.typeCode)
			if err != nil {
				t.Error(err)
			}
			if !isEqual(got, test.want) {
				t.Errorf("encodeValue(%v) = %v, want = %v", test.value, got, test.want)
			}
		})
	}
}

func TestGetCopyStats(t *testing.T) {
	tests := []struct {
		desc    string
		columns []Column
		rows    int
		header  bool
		error   string
	}{
		{
			desc:    "basic no header",
			columns: []Column{{Name: "Name"}, {Name: "Age", Nullable: true}},
			rows:    2,
			header:  false,
			error:   "",
		},
		{
			desc:    "basic with header",
			columns: []Column{{Name: "Name"}, {Name: "Age", Nullable: true}},
			rows:    10,
			header:  true,
			error:   "",
		},
		{
			desc:    "illegal column count",
			columns: []Column{{Name: "Name"}},
			rows:    20,
			header:  true,
			error:   "illegal number of columns",
		},
		{
			desc:    "wrong column name",
			columns: []Column{{Name: "Name"}, {Name: "Address", Nullable: true}},
			rows:    15,
			header:  true,
			error:   "column `Age` didn't match expected column `Address`",
		},
		{
			desc:    "non-nullable column",
			columns: []Column{{Name: "Name"}, {Name: "Age", Nullable: false}},
			rows:    10,
			header:  true,
			error:   "cannot use null value for non-nullable column",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			tmpfile, err := os.CreateTemp("", "test.csv")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())

			writer := csv.NewWriter(tmpfile)
			if test.header {
				writer.Write([]string{"Name", "Age"})
			}
			for i := 0; i < test.rows; i++ {
				if i%3 == 2 {
					writer.Write([]string{fmt.Sprintf("%c", 'a'+i), ""})
				} else {
					writer.Write([]string{fmt.Sprintf("%c", 'a'+i), fmt.Sprintf("%d", i)})
				}
			}
			writer.Flush()

			copier := NewCopier("Users", test.columns, tmpfile.Name(), test.header)
			stats, err := copier.getCopyStats()
			if err != nil {
				if test.error == "" {
					t.Error(err)
				} else if !strings.Contains(err.Error(), test.error) {
					t.Errorf("getCopyStats() error = %v, want = %s", err.Error(), test.error)
				}
				return
			}
			if stats.Rows != test.rows {
				t.Errorf("getCopyStats() stats.Rows = %d, want = %d", stats.Rows, test.rows)
			}
			expectedBatch := maxMutationBatchSize
			if test.rows < maxMutationBatchSize {
				expectedBatch = test.rows

			}
			if stats.MutationBatchSize != expectedBatch {
				t.Errorf("getCopyStats() stats.MutationBatchSize = %d, want = %d", stats.MutationBatchSize, expectedBatch)
			}
		})
	}
}

func TestCopyE2E(t *testing.T) {
	server := setupTestServer(t)

	ddl, err := spansql.ParseDDL("", "CREATE TABLE Users (Name STRING(MAX) NOT NULL, Age INT64) PRIMARY KEY (Name)")
	if err != nil {
		t.Fatalf("failed to parse DDL: %v", err)
	}
	if err := server.UpdateDDL(ddl); err != nil {
		t.Fatalf("failed to update DDL: %v", err)
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, server.Addr, opts...)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	session, err := NewSession("project", "instance", "database", pb.RequestOptions_PRIORITY_UNSPECIFIED, "role", nil, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("failed to create spanner-cli session: %v", err)
	}

	tmpfile, err := os.CreateTemp("", "test.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	writer := csv.NewWriter(tmpfile)
	writer.Write([]string{"Name", "Age"})
	writer.Write([]string{"John", "32"})
	writer.Write([]string{"Bill", "31"})
	writer.Write([]string{"Matt", "22"})
	writer.Flush()

	columns := []Column{{Name: "Name", Type: sppb.TypeCode_STRING}, {Name: "Age", Type: sppb.TypeCode_INT64, Nullable: true}}

	copier := NewCopier("Users", columns, tmpfile.Name(), true)
	stats, err := copier.Copy(ctx, session)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Rows != 3 {
		t.Errorf("Copy() stats.Rows = %d, want = 3", stats.Rows)
	}

	if stats.MutationBatchSize != 3 {
		t.Errorf("Copy() stats.MutationBatchSize = %d, want = 3", stats.MutationBatchSize)
	}

	if stats.WorkerCount != 1 {
		t.Errorf("Copy() stats.WorkerCount = %d, want = 1", stats.WorkerCount)
	}

	type testUserRow struct {
		Name string
		Age  int64
	}

	var got []testUserRow
	iter := session.client.Single().Query(ctx, spanner.NewStatement("SELECT * FROM Users ORDER BY Name"))
	if err := iter.Do(func(r *spanner.Row) error {
		var u testUserRow
		if err := r.ToStruct(&u); err != nil {
			t.Fatalf("failed to parse user row: %v", err)
		}
		got = append(got, u)
		return nil
	}); err != nil {
		t.Fatalf("failed to run query: %v", err)
	}
	expected := []testUserRow{
		{"Bill", 31},
		{"John", 32},
		{"Matt", 22},
	}
	if !cmp.Equal(got, expected) {
		t.Errorf("diff: %s", cmp.Diff(got, expected))
	}
}
