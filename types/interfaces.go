package types

import (
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/shopspring/decimal"
)

type RowRecord interface {
	Schema() StructType
	Length() int
	IsNullAt(fieldName string) (bool, error)

	GetInt(fieldName string) (int, error)
	GetInt64(fieldName string) (int64, error)
	GetByte(fieldName string) (int8, error)
	GetShort(fieldName string) (int16, error)
	GetBoolean(fieldName string) (bool, error)
	GetFloat(fieldName string) (float32, error)
	GetDouble(fieldName string) (float64, error)
	GetString(fieldName string) (string, error)
	GetBinary(fieldName string) ([]byte, error)
	GetBigDecimal(fieldName string) (decimal.Decimal, error)
	GetTimestamp(fieldName string) (time.Time, error)
	GetDate(fieldName string) (time.Time, error)
	GetRecord(fieldName string) (RowRecord, error)
	GetList(fieldName string) ([]any, error)
	GetMap(fieldName string) (map[any]any, error)
}

type DataType interface {
	Name() string
}

type Expression interface {
	// Eval returns the result of evaluating this expression on the given input RowRecord.
	Eval(record RowRecord) (any, error)

	// DataType returns the DataType of the result of evaluating this expression.
	DataType() DataType

	// String returns the String representation of this expression.
	String() string

	// Children returns List of the immediate children of this node
	Children() []Expression

	// References returns the set of column names
	References() []string
}

// References returns the names of columns referenced by this expression.
func References(e Expression) mapset.Set[string] {
	res := mapset.NewSet[string]()
	for _, c := range e.Children() {
		for ele := range References(c).Iterator().C {
			res.Add(ele)
		}
	}
	return res
}
