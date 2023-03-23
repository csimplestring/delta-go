package types

import "fmt"

type BinaryType struct {
}

func (b *BinaryType) Name() string {
	return "binary"
}

type BooleanType struct {
}

func (b *BooleanType) Name() string {
	return "boolean"
}

type ByteType struct {
}

func (b *ByteType) Name() string {
	return "tinyint"
}

type DateType struct {
}

func (d *DateType) Name() string {
	return "date"
}

type DecimalType struct {
	Precision int `json:"precision,omitempty"`
	Scale     int `json:"scale,omitempty"`
}

func (d *DecimalType) Name() string {
	return "decimal"
}

func (d *DecimalType) JSON() string {
	return fmt.Sprintf("decimal(%d,%d)", d.Precision, d.Scale)
}

type DoubleType struct {
}

func (d *DoubleType) Name() string {
	return "double"
}

type FloatType struct {
}

func (f *FloatType) Name() string {
	return "float"
}

type IntegerType struct {
}

func (i *IntegerType) Name() string {
	return "int"
}

type LongType struct {
}

func (l *LongType) Name() string {
	return "bigint"
}

type NullType struct {
}

func (n *NullType) Name() string {
	return "null"
}

type ShortType struct {
}

func (s *ShortType) Name() string {
	return "smallint"
}

type StringType struct {
}

func (s *StringType) Name() string {
	return "string"
}

type TimestampType struct {
}

func (t *TimestampType) Name() string {
	return "timestamp"
}
