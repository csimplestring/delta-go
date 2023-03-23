package types

import (
	"fmt"
)

type unaryExp struct {
	Child        Expression
	nullSafeEval func(r any) (any, error)
}

// Eval returns the result of evaluating this expression on the given input RowRecord.
func (u *unaryExp) Eval(record RowRecord) (any, error) {
	res, err := u.Child.Eval(record)
	if res == nil || err != nil {
		return nil, err
	}
	return u.nullSafeEval(res)
}

// DataType returns the DataType of the result of evaluating this expression.
func (u *unaryExp) DataType() DataType {
	return &BooleanType{}
}

// String returns the String representation of this expression.
func (u *unaryExp) String() string {
	panic("not implemented") // TODO: Implement
}

func (u *unaryExp) References() []string {
	return nil
}

// Children returns List of the immediate children of this node
func (u *unaryExp) Children() []Expression {
	return []Expression{u.Child}
}

type IsNull struct {
	*unaryExp
}

func (i *IsNull) String() string {
	return fmt.Sprintf("(%s) IS NULL", i.Child.String())
}

func (i *IsNull) Eval(record RowRecord) (any, error) {
	res, err := i.Child.Eval(record)
	return res == nil, err
}

func NewIsNull(e Expression) *IsNull {
	u := &unaryExp{
		Child: e,
	}
	return &IsNull{u}
}

type Not struct {
	*unaryExp
}

func (n *Not) String() string {
	return fmt.Sprintf("(NOT %s)", n.Child.String())
}

func NewNot(e Expression) *Not {
	u := &unaryExp{
		Child: e,
		nullSafeEval: func(r any) (any, error) {
			return !r.(bool), nil
		},
	}
	return &Not{u}
}

type IsNotNull struct {
	*unaryExp
}

func (i *IsNotNull) String() string {
	return fmt.Sprintf("(%s) IS NOT NULL", i.Child.String())
}

func (i *IsNotNull) Eval(record RowRecord) (any, error) {
	res, err := i.Child.Eval(record)
	if err != nil {
		return nil, err
	}
	return res != nil, nil
}

func NewIsNotNull(e Expression) *IsNotNull {
	u := &unaryExp{
		Child: e,
	}
	return &IsNotNull{u}
}
