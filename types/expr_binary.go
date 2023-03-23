package types

import (
	"fmt"
	"time"

	"github.com/csimplestring/delta-go/errno"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/rotisserie/eris"
	"github.com/shopspring/decimal"
)

type binaryExp struct {
	Left         Expression
	Right        Expression
	Symbol       string
	nullSafeEval func(l any, r any) (any, error)
}

func (b *binaryExp) Eval(record RowRecord) (any, error) {
	leftRes, err := b.Left.Eval(record)
	if err != nil || leftRes == nil {
		return nil, err
	}

	rightRes, err := b.Right.Eval(record)
	if err != nil || rightRes == nil {
		return nil, err
	}

	return b.nullSafeEval(leftRes, rightRes)
}

func (b *binaryExp) Children() []Expression {
	return []Expression{b.Left, b.Right}
}

func (b *binaryExp) DataType() DataType {
	return &BooleanType{}
}

func (b *binaryExp) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left.String(), b.Symbol, b.Right.String())
}

func (b *binaryExp) References() []string {
	res := mapset.NewSet[string]()
	for _, ch := range b.Children() {
		for _, c := range ch.References() {
			res.Add(c)
		}
	}
	return res.ToSlice()
}

type primitiveType interface {
	int8 | int16 | int | int64 | byte | string | float32 | float64
}

func primitiveCompare[T primitiveType](l any, r any) int {
	if l.(T) == r.(T) {
		return 0
	} else if l.(T) < r.(T) {
		return -1
	} else {
		return 1
	}
}

func compareWithType(dataType DataType, l any, r any) (int, error) {
	switch dataType.(type) {
	case *IntegerType:
		return primitiveCompare[int](l, r), nil
	case *FloatType:
		return primitiveCompare[float32](l, r), nil
	case *LongType:
		return primitiveCompare[int64](l, r), nil
	case *ByteType:
		return primitiveCompare[byte](l, r), nil
	case *ShortType:
		return primitiveCompare[int8](l, r), nil
	case *DoubleType:
		return primitiveCompare[float64](l, r), nil
	case *StringType:
		return primitiveCompare[string](l, r), nil
	case *DecimalType:
		return l.(decimal.Decimal).Cmp(r.(decimal.Decimal)), nil
	case *DateType:
		return compareTime(l.(time.Time), r.(time.Time)), nil
	case *TimestampType:
		return compareTime(l.(time.Time), r.(time.Time)), nil
	case *BooleanType:
		return compareBool(l.(bool), r.(bool)), nil
	case *BinaryType:
		return compareBinary(l.([]byte), r.([]byte)), nil
	default:
		illegalArgErr := &errno.IllegalArgError{Msg: fmt.Sprintf("unsupported type %s", dataType)}
		return 0, eris.Wrap(illegalArgErr, "")
	}
}

func compareBinary(b1 []byte, b2 []byte) int {
	i := 0
	for i < len(b1) && i < len(b2) {
		if b1[i] != b2[i] {
			return primitiveCompare[byte](b1[i], b2[i])
		}
		i++
	}
	return primitiveCompare[int](len(b1), len(b2))
}

func compareBool(b1 bool, b2 bool) int {
	if b1 == b2 {
		return 0
	}
	if b1 {
		return 1
	} else {
		return -1
	}
}

func compareTime(t1 time.Time, t2 time.Time) int {
	if t1.Equal(t2) {
		return 0
	}
	if t1.Before(t2) {
		return -1
	} else {
		return 1
	}
}

type And struct {
	*binaryExp
}

func NewAnd(l Expression, r Expression) *And {
	b := &binaryExp{
		Left:   l,
		Right:  r,
		Symbol: "&&",
		nullSafeEval: func(lRes any, rRes any) (any, error) {
			return lRes.(bool) && rRes.(bool), nil
		},
	}

	return &And{
		binaryExp: b,
	}
}

type Or struct {
	*binaryExp
}

func NewOr(l Expression, r Expression) *Or {
	b := &binaryExp{
		Left:   l,
		Right:  r,
		Symbol: "||",
		nullSafeEval: func(lRes any, rRes any) (any, error) {
			return lRes.(bool) || rRes.(bool), nil
		},
	}

	return &Or{
		binaryExp: b,
	}
}

type EqualTo struct {
	*binaryExp
}

func NewEqualTo(l Expression, r Expression) *EqualTo {
	b := &binaryExp{
		Left:   l,
		Right:  r,
		Symbol: "=",
		nullSafeEval: func(lRes any, rRes any) (any, error) {
			res, err := compareWithType(l.DataType(), lRes, rRes)
			return res == 0, err
		},
	}

	return &EqualTo{
		binaryExp: b,
	}
}

type Gt struct {
	*binaryExp
}

func NewGreaterThan(l Expression, r Expression) *Gt {
	b := &binaryExp{
		Left:   l,
		Right:  r,
		Symbol: ">",
		nullSafeEval: func(lRes any, rRes any) (any, error) {
			res, err := compareWithType(l.DataType(), lRes, rRes)
			return res > 0, err
		},
	}

	return &Gt{
		binaryExp: b,
	}
}

type Gte struct {
	*binaryExp
}

func NewGreaterThanOrEq(l Expression, r Expression) *Gte {
	b := &binaryExp{
		Left:   l,
		Right:  r,
		Symbol: ">=",
		nullSafeEval: func(lRes any, rRes any) (any, error) {
			res, err := compareWithType(l.DataType(), lRes, rRes)
			return res >= 0, err
		},
	}

	return &Gte{
		binaryExp: b,
	}
}

type Lt struct {
	*binaryExp
}

func NewLessThan(l Expression, r Expression) *Lt {
	b := &binaryExp{
		Left:   l,
		Right:  r,
		Symbol: "<",
		nullSafeEval: func(lRes any, rRes any) (any, error) {
			res, err := compareWithType(l.DataType(), lRes, rRes)
			return res < 0, err
		},
	}

	return &Lt{
		binaryExp: b,
	}
}

type Lte struct {
	*binaryExp
}

func NewLessThanOrEq(l Expression, r Expression) *Lte {
	b := &binaryExp{
		Left:   l,
		Right:  r,
		Symbol: "<=",
		nullSafeEval: func(lRes any, rRes any) (any, error) {
			res, err := compareWithType(l.DataType(), lRes, rRes)
			return res <= 0, err
		},
	}

	return &Lte{
		binaryExp: b,
	}
}
