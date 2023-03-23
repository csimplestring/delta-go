package types

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func testPredicate(t *testing.T, p Expression, expected any, record RowRecord) {
	actual, err := p.Eval(record)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestOr(t *testing.T) {

	testPredicate(t, NewOr(LiteralNull(&BooleanType{}), False), nil, nil)
	testPredicate(t, NewOr(False, LiteralNull(&BooleanType{})), nil, nil)
	testPredicate(t, NewOr(True, LiteralNull(&BooleanType{})), nil, nil)

	testPredicate(t, NewOr(LiteralNull(&BooleanType{}), LiteralNull(&BooleanType{})), nil, nil)
	testPredicate(t, NewOr(False, False), false, nil)
	testPredicate(t, NewOr(True, False), true, nil)
	testPredicate(t, NewOr(False, True), true, nil)
	testPredicate(t, NewOr(True, True), true, nil)
}

func TestAnd(t *testing.T) {

	testPredicate(t, NewAnd(LiteralNull(&BooleanType{}), False), nil, nil)
	testPredicate(t, NewAnd(False, LiteralNull(&BooleanType{})), nil, nil)
	testPredicate(t, NewAnd(True, LiteralNull(&BooleanType{})), nil, nil)

	testPredicate(t, NewAnd(LiteralNull(&BooleanType{}), LiteralNull(&BooleanType{})), nil, nil)
	testPredicate(t, NewAnd(False, False), false, nil)
	testPredicate(t, NewAnd(True, False), false, nil)
	testPredicate(t, NewAnd(False, True), false, nil)
	testPredicate(t, NewAnd(True, True), true, nil)
}

func TestComparison(t *testing.T) {

	type expected struct {
		fn         func(a Expression, b Expression) Expression
		smallBig   bool
		bigSmall   bool
		smallSmall bool
	}
	expecteds := []expected{
		{func(a, b Expression) Expression { return NewLessThan(a, b) }, true, false, false},
		{func(a, b Expression) Expression { return NewLessThanOrEq(a, b) }, true, false, true},
		{func(a, b Expression) Expression { return NewGreaterThan(a, b) }, false, true, false},
		{func(a, b Expression) Expression { return NewGreaterThanOrEq(a, b) }, false, true, true},
		{func(a, b Expression) Expression { return NewEqualTo(a, b) }, false, false, true},
	}

	type input struct {
		small   Expression
		big     Expression
		small2  Expression
		nullLit Expression
	}
	inputs := []input{
		{LiteralInt(1), LiteralInt(2), LiteralInt(1), LiteralNull(&IntegerType{})},
		{LiteralFloat(1), LiteralFloat(2), LiteralFloat(1), LiteralNull(&FloatType{})},
		{LiteralLong(1), LiteralLong(2), LiteralLong(1), LiteralNull(&LongType{})},
		{LiteralShort(1), LiteralShort(2), LiteralShort(1), LiteralNull(&ShortType{})},
		{LiteralDouble(1), LiteralDouble(2), LiteralDouble(1), LiteralNull(&DoubleType{})},
		{LiteralByte(1), LiteralByte(2), LiteralByte(1), LiteralNull(&ByteType{})},

		{LiteralBigDecimal(decimal.RequireFromString("123.45")),
			LiteralBigDecimal(decimal.RequireFromString("887.62")),
			LiteralBigDecimal(decimal.RequireFromString("123.45")),
			LiteralNull(&DecimalType{Precision: 5, Scale: 2})},

		{False, True, False, LiteralNull(&BooleanType{})},
		{LiteralTimestamp(time.Unix(0, 0)), LiteralTimestamp(time.Unix(10000, 0)),
			LiteralTimestamp(time.Unix(0, 0)), LiteralNull(&TimestampType{})},

		{LiteralString("apples"), LiteralString("oranges"), LiteralString("apples"), LiteralNull(&StringType{})},
		{LiteralBinary([]byte("apples")), LiteralBinary([]byte("oranges")), LiteralBinary([]byte("apples")), LiteralNull(&BinaryType{})},
	}

	for _, input := range inputs {
		for _, expect := range expecteds {
			testPredicate(t, expect.fn(input.small, input.big), expect.smallBig, nil)
			testPredicate(t, expect.fn(input.big, input.small), expect.bigSmall, nil)
			testPredicate(t, expect.fn(input.small, input.small2), expect.smallSmall, nil)
			testPredicate(t, expect.fn(input.small, input.nullLit), nil, nil)
			testPredicate(t, expect.fn(input.nullLit, input.small), nil, nil)
		}
	}

	// todo: there are some extensive comparison test for databricks SQL
}
