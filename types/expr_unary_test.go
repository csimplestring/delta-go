package types

import (
	"testing"
)

func TestNot(t *testing.T) {
	testPredicate(t, NewNot(False), true, nil)
	testPredicate(t, NewNot(True), false, nil)
	testPredicate(t, NewNot(LiteralNull(&BooleanType{})), nil, nil)
}

func TestIsNull(t *testing.T) {
	testPredicate(t, NewIsNull(LiteralNull(&BooleanType{})), true, nil)
	testPredicate(t, NewIsNull(False), false, nil)
}

func TestIsNotNull(t *testing.T) {
	testPredicate(t, NewIsNotNull(LiteralNull(&BooleanType{})), false, nil)
	testPredicate(t, NewIsNotNull(False), true, nil)
}
