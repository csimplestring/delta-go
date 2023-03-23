package types

import (
	"fmt"
)

type Column struct {
	Name         string
	Type         DataType
	nullSafeEval func(r RowRecord) (any, error)
}

func NewColumn(name string, t DataType) *Column {
	col := &Column{
		Name: name,
		Type: t,
	}

	switch t.(type) {
	case *IntegerType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetInt(name) }
	case *LongType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetInt64(name) }
	case *ByteType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetByte(name) }
	case *ShortType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetShort(name) }
	case *BooleanType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetBoolean(name) }
	case *FloatType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetFloat(name) }
	case *DoubleType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetDouble(name) }
	case *StringType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetString(name) }
	case *BinaryType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetBinary(name) }
	case *DecimalType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetBigDecimal(name) }
	case *TimestampType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetTimestamp(name) }
	case *DateType:
		col.nullSafeEval = func(r RowRecord) (any, error) { return r.GetDate(name) }
	default:
		panic("Column: unsupported data type")
	}

	return col
}

// Eval returns the result of evaluating this expression on the given input RowRecord.
func (c *Column) Eval(record RowRecord) (any, error) {
	isNull, err := record.IsNullAt(c.Name)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}

	return c.nullSafeEval(record)
}

// DataType returns the DataType of the result of evaluating this expression.
func (c *Column) DataType() DataType {
	return c.Type
}

// String returns the String representation of this expression.
func (c *Column) String() string {
	return fmt.Sprintf("Column(%s)", c.Name)
}

// Children returns List of the immediate children of this node
func (c *Column) Children() []Expression {
	return []Expression{}
}

func (c *Column) References() []string {
	return []string{c.Name}
}
