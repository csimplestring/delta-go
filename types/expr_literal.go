package types

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type Literal struct {
	Value any
	Type  DataType
}

// Eval returns the result of evaluating this expression on the given input RowRecord.
func (l *Literal) Eval(record RowRecord) (any, error) {
	return l.Value, nil
}

// DataType returns the DataType of the result of evaluating this expression.
func (l *Literal) DataType() DataType {
	return l.Type
}

// String returns the String representation of this expression.
func (l *Literal) String() string {
	return fmt.Sprintf("%v", l.Value)
}

// Children returns List of the immediate children of this node
func (l *Literal) Children() []Expression {
	return []Expression{}
}

func (l *Literal) References() []string {
	return nil
}

var True *Literal = &Literal{Value: true, Type: &BooleanType{}}
var False *Literal = &Literal{Value: false, Type: &BooleanType{}}

func LiteralInt(n int) *Literal {
	return &Literal{Value: n, Type: &IntegerType{}}
}

func LiteralBinary(b []byte) *Literal {
	return &Literal{Value: b, Type: &BinaryType{}}
}

func LiteralDate(d time.Time) *Literal {
	return &Literal{Value: d, Type: &DateType{}}
}

func LiteralBigDecimal(d decimal.Decimal) *Literal {
	return &Literal{Value: d, Type: &DecimalType{}}
}

func LiteralDouble(d float64) *Literal {
	return &Literal{Value: d, Type: &DoubleType{}}
}

func LiteralFloat(f float32) *Literal {
	return &Literal{Value: f, Type: &FloatType{}}
}

func LiteralLong(n int64) *Literal {
	return &Literal{Value: n, Type: &LongType{}}
}

func LiteralShort(n int8) *Literal {
	return &Literal{Value: n, Type: &ShortType{}}
}

func LiteralString(s string) *Literal {
	return &Literal{Value: s, Type: &StringType{}}
}

func LiteralTimestamp(t time.Time) *Literal {
	return &Literal{Value: t, Type: &TimestampType{}}
}

func LiteralByte(b byte) *Literal {
	return &Literal{Value: b, Type: &ByteType{}}
}

func LiteralNull(dt DataType) *Literal {
	return &Literal{Value: nil, Type: dt}
}
