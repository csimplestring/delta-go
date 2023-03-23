package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIs(t *testing.T) {
	a1 := Is[*IntegerType](&IntegerType{})
	a2 := Is[*IntegerType](&DoubleType{})
	assert.True(t, a1)
	assert.False(t, a2)
}

func Test_checkFieldNames(t *testing.T) {

	badCharacters := " ,;{}()\n\t="
	for _, char := range badCharacters {
		for _, name := range []string{fmt.Sprintf("a%cb", char), fmt.Sprintf("%cab", char), fmt.Sprintf("ab%c", char)} {
			err := CheckFieldNames([]string{name})
			assert.Error(t, err, fmt.Sprintf("input: %s", name))
		}
	}

	goodCharacters := "#.`!@$%^&*~_<>?/:"
	for _, char := range goodCharacters {
		assert.NoError(t,
			CheckFieldNames([]string{
				fmt.Sprintf("a%cb", char),
				fmt.Sprintf("%cab", char),
				fmt.Sprintf("ab%c", char)}))
	}
}

func testDatatypeChange(t *testing.T, scenario string, makeFn func(DataType) *StructType) {
	schemas := map[string]*StructType{
		"int":    makeFn(&IntegerType{}),
		"string": makeFn(&StringType{}),
		"struct": makeFn(new(StructType).Add2("a", &StringType{})),
		"array":  makeFn(&ArrayType{ElementType: &IntegerType{}, ContainsNull: true}),
		"map":    makeFn(&MapType{KeyType: &StringType{}, ValueType: &FloatType{}, ValueContainsNull: true}),
	}

	for k1, v1 := range schemas {
		for k2, v2 := range schemas {
			if k1 != k2 {
				assert.False(t, IsWriteCompatible(v1, v2), scenario)
			}
		}
	}
}

func testNullability(t *testing.T, scenario string, makeFn func(bool) *StructType) {
	nullable := makeFn(true)
	nonNullable := makeFn(false)

	assert.False(t, IsWriteCompatible(nullable, nonNullable), scenario)
	assert.True(t, IsWriteCompatible(nonNullable, nullable), scenario)
}

func testColumnVariantions(t *testing.T, scenario string, makeFn func(fn func(*StructType) *StructType) *StructType) {

	withoutExtra := makeFn(func(st *StructType) *StructType {
		return st
	})
	withExtraNullable := makeFn(func(st *StructType) *StructType {
		return st.Add2("extra", &StringType{})
	})
	withExtraMixedCase := makeFn(func(st *StructType) *StructType {
		return st.Add2("eXtRa", &StringType{})
	})
	withExtraNonNullable := makeFn(func(st *StructType) *StructType {
		return st.Add3("extra", &StringType{}, false)
	})

	assert.False(t, IsWriteCompatible(withExtraNullable, withoutExtra), "dropping a field should fail write compatibility")
	assert.True(t, IsWriteCompatible(withoutExtra, withExtraNullable), "adding a nullable field should not fail write compatibility")
	assert.True(t, IsWriteCompatible(withoutExtra, withExtraNonNullable), "add a non-nullable field should not fail write compatibility")
	assert.False(t, IsWriteCompatible(withExtraNullable, withExtraMixedCase), "case variation of field name should fail write compatibility")

	testNullability(t, scenario, func(b bool) *StructType {
		return makeFn(func(st *StructType) *StructType {
			return st.Add3("extra", &StringType{}, b)
		})
	})
	testDatatypeChange(t, scenario, func(dt DataType) *StructType {
		return makeFn(func(st *StructType) *StructType {
			return st.Add2("extra", dt)
		})
	})
}

func TestColumnVariantions(t *testing.T) {
	testColumnVariantions(t, "top level", func(fn func(*StructType) *StructType) *StructType {
		return fn((&StructType{}).Add2("a", &IntegerType{}))
	})
	testColumnVariantions(t, "nested struct", func(fn func(*StructType) *StructType) *StructType {
		return fn((&StructType{}).Add2("a", (&StructType{}).Add2("b", &IntegerType{})))
	})
	testColumnVariantions(t, "nested in array", func(fn func(*StructType) *StructType) *StructType {
		return fn((&StructType{}).Add2("array", &ArrayType{ElementType: fn((&StructType{}).Add2("b", &IntegerType{})), ContainsNull: true}))
	})
	testColumnVariantions(t, "nested in map key", func(fn func(*StructType) *StructType) *StructType {
		return fn((&StructType{}).Add2("map", &MapType{
			KeyType:           fn((&StructType{}).Add2("b", &IntegerType{})),
			ValueType:         &StringType{},
			ValueContainsNull: true,
		}))
	})
	testColumnVariantions(t, "nested in map value", func(fn func(*StructType) *StructType) *StructType {
		return fn((&StructType{}).Add2("map", &MapType{
			KeyType:           &StringType{},
			ValueType:         fn((&StructType{}).Add2("b", &IntegerType{})),
			ValueContainsNull: true,
		}))
	})
}

func Test_isWriteCompatible(t *testing.T) {
	testNullability(t, "array contains null", func(b bool) *StructType {
		return (&StructType{}).Add2("array", &ArrayType{ElementType: &StringType{}, ContainsNull: b})
	})
	testNullability(t, "map contains null values", func(b bool) *StructType {
		return (&StructType{}).Add2("map", &MapType{KeyType: &IntegerType{}, ValueType: &StringType{}, ValueContainsNull: b})
	})
	testNullability(t, "map nested in array", func(b bool) *StructType {
		return (&StructType{}).Add2("map", &ArrayType{ElementType: &MapType{KeyType: &IntegerType{}, ValueType: &StringType{}, ValueContainsNull: b}, ContainsNull: b})
	})
	testNullability(t, "array nested in map", func(b bool) *StructType {
		return (&StructType{}).Add2("map", &MapType{KeyType: &IntegerType{}, ValueType: &ArrayType{ElementType: &StringType{}, ContainsNull: b}, ValueContainsNull: b})
	})

	testDatatypeChange(t, "array element", func(dt DataType) *StructType {
		return (&StructType{}).Add2("array", &ArrayType{ElementType: dt, ContainsNull: true})
	})
	testDatatypeChange(t, "map key", func(dt DataType) *StructType {
		return (&StructType{}).Add2("map", &MapType{KeyType: dt, ValueType: &StringType{}, ValueContainsNull: true})
	})
	testDatatypeChange(t, "map value", func(dt DataType) *StructType {
		return (&StructType{}).Add2("map", &MapType{KeyType: &StringType{}, ValueType: dt, ValueContainsNull: true})
	})

}

func TestExplodeNestedFieldNames(t *testing.T) {
	s := NewStructType([]*StructField{
		NewStructField("a", &IntegerType{}, true),
		NewStructField("b", &ArrayType{ElementType: &IntegerType{}, ContainsNull: true}, true),
		NewStructField("c", NewStructType([]*StructField{
			NewStructField("c0", &IntegerType{}, true),
			NewStructField("c1", &ArrayType{ElementType: &IntegerType{}, ContainsNull: true}, true),
		}), true),
		NewStructField("d",
			&MapType{KeyType: &IntegerType{}, ValueType: &StringType{}, ValueContainsNull: true}, true),
	})

	fields := ExplodeNestedFieldNames(s)
	assert.Equal(t, []string{"a", "b", "c", "c.c0", "c.c1", "d"}, fields)
}

func TestCheckColumnNameDuplication(t *testing.T) {
	type args struct {
		schema  *StructType
		colType string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"duplicate column name in top level",
			args{
				NewStructType([]*StructField{
					NewStructField("dupColName", &IntegerType{}, true),
					NewStructField("a", &IntegerType{}, true),
					NewStructField("dupColName", &IntegerType{}, true),
				}),
				"",
			},
		},
		{
			"duplicate column name in top level - case sensitivity",
			args{
				NewStructType([]*StructField{
					NewStructField("dupColName", &IntegerType{}, true),
					NewStructField("a", &IntegerType{}, true),
					NewStructField("dupCOLNAME", &IntegerType{}, true),
				}),
				"",
			},
		},
		{
			"duplicate column name for nested column + non-nested column",
			args{
				NewStructType([]*StructField{
					NewStructField("dupColName", NewStructType([]*StructField{
						NewStructField("a", &IntegerType{}, true),
						NewStructField("b", &IntegerType{}, true),
					}), true),
					NewStructField("dupColName", &IntegerType{}, true),
				}),
				"",
			},
		},
		{
			"duplicate column name for nested column + non-nested column - case sensitivity",
			args{
				NewStructType([]*StructField{
					NewStructField("dupColName", NewStructType([]*StructField{
						NewStructField("a", &IntegerType{}, true),
						NewStructField("b", &IntegerType{}, true),
					}), true),
					NewStructField("dupCOLNAME", &IntegerType{}, true),
				}),
				"",
			},
		},
		{
			"duplicate column name in nested level",
			args{
				NewStructType([]*StructField{
					NewStructField("top", NewStructType([]*StructField{
						NewStructField("a", &IntegerType{}, true),
						NewStructField("b", &IntegerType{}, true),
						NewStructField("a", &StringType{}, true),
					}), true),
				}),
				"",
			},
		},
		{
			"duplicate column name in nested level - case sensitivity",
			args{
				NewStructType([]*StructField{
					NewStructField("top", NewStructType([]*StructField{
						NewStructField("a", &IntegerType{}, true),
						NewStructField("b", &IntegerType{}, true),
						NewStructField("A", &StringType{}, true),
					}), true),
				}),
				"",
			},
		},
		{
			"duplicate column name in double nested level",
			args{
				NewStructType([]*StructField{
					NewStructField("top", NewStructType([]*StructField{
						NewStructField("b", &StructType{
							Fields: []*StructField{
								NewStructField("dupColName", &IntegerType{}, true),
								NewStructField("c", &IntegerType{}, true),
								NewStructField("dupColName", &IntegerType{}, true),
							},
						}, true),
						NewStructField("d", &IntegerType{}, true),
					}), true),
				}),
				"",
			},
		},
		{
			"duplicate column name in double nested array",
			args{
				NewStructType([]*StructField{
					NewStructField("top", NewStructType([]*StructField{
						NewStructField("b", &ArrayType{
							ElementType: NewStructType(
								[]*StructField{
									NewStructField("dupColName", &IntegerType{}, true),
									NewStructField("c", &IntegerType{}, true),
									NewStructField("dupColName", &IntegerType{}, true),
								},
							)}, true),
						NewStructField("d", &IntegerType{}, true),
					}), true),
				}),
				"",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckColumnNameDuplication(tt.args.schema, tt.args.colType)
			assert.Error(t, err)
		})
	}
}
