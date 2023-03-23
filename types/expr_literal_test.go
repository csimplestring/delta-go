package types

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestLiteral(t *testing.T) {

	testLiteral(True, true, t)
	testLiteral(False, false, t)
	testLiteral(LiteralByte(byte(8)), byte(8), t)
	testLiteral(LiteralFloat(float32(1.0)), float32(1.0), t)
	testLiteral(LiteralDouble(float64(1.0)), float64(1.0), t)
	testLiteral(LiteralInt(5), 5, t)
	testLiteral(LiteralLong(int64(10)), int64(10), t)

	testLiteral(LiteralNull(&BooleanType{}), nil, t)
	testLiteral(LiteralNull(&IntegerType{}), nil, t)
	testLiteral(LiteralShort(5), int8(5), t)
	testLiteral(LiteralString("test"), "test", t)

	now := time.Now()
	testLiteral(LiteralTimestamp(now), now, t)
	testLiteral(LiteralDate(now), now, t)
	testLiteral(LiteralBigDecimal(decimal.NewFromFloat(0.1)), decimal.NewFromFloat(0.1), t)
	testLiteral(LiteralBinary([]byte("test")), []byte("test"), t)

}

func testLiteral(l *Literal, expected any, t *testing.T) {
	res, err := l.Eval(nil)
	assert.NoError(t, err)
	assert.Equal(t, expected, res)
}
