package types

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/csimplestring/delta-go/errno"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/repeale/fp-go"
	"github.com/samber/mo"
)

func Is[T DataType](a DataType) bool {
	_, ok := a.(T)
	return ok
}

func CheckFieldNames(names []string) error {
	return parquetSchemaConverterObj.checkFieldNames(names)
}

func CheckUnenforceableNotNullConstraints(schema *StructType) error {
	return traverseColumns([]string{}, schema)
}

func CheckField(path []string, f *StructField) error {
	switch t := f.DataType.(type) {
	case *ArrayType:
		a := t
		if !matchesNullableType(a.ElementType) {
			return errno.NestedNotNullConstraintError(prettyFieldName(append(path, f.Name)), ForceToJSON(a.ElementType), "element")
		}
	case *MapType:
		m := t
		keyTypeNullable := matchesNullableType(m.KeyType)
		valueTypeNullable := matchesNullableType(m.ValueType)
		if !keyTypeNullable {
			return errno.NestedNotNullConstraintError(prettyFieldName(append(path, f.Name)), ForceToJSON(m.KeyType), "key")
		}
		if !valueTypeNullable {
			return errno.NestedNotNullConstraintError(prettyFieldName(append(path, f.Name)), ForceToJSON(m.ValueType), "value")
		}
	default:
		return nil
	}
	return nil
}

func traverseColumns(path []string, dt DataType) error {
	switch t := dt.(type) {
	case *StructType:
		s := t
		for _, field := range s.Fields {
			if err := CheckField(path, field); err != nil {
				return err
			}
			if err := traverseColumns(append(path, field.Name), field.DataType); err != nil {
				return err
			}
		}
	case *ArrayType:
		a := t
		return traverseColumns(append(path, "element"), a.ElementType)
	case *MapType:
		m := t
		if err := traverseColumns(append(path, "key"), m.KeyType); err != nil {
			return err
		}
		return traverseColumns(append(path, "value"), m.ValueType)
	default:
		return nil
	}
	return nil
}

func isDatatypeWriteCompatible(_existingType DataType, _newType DataType) bool {

	if Is[*StructType](_existingType) && Is[*StructType](_newType) {
		return IsWriteCompatible(_existingType.(*StructType), _newType.(*StructType))
	}
	if Is[*ArrayType](_existingType) && Is[*ArrayType](_newType) {
		e := _existingType.(*ArrayType)
		n := _newType.(*ArrayType)
		return (!e.ContainsNull || n.ContainsNull) && isDatatypeWriteCompatible(e.ElementType, n.ElementType)
	}
	if Is[*MapType](_existingType) && Is[*MapType](_newType) {
		e := _existingType.(*MapType)
		n := _newType.(*MapType)
		return (!e.ValueContainsNull || n.ValueContainsNull) &&
			isDatatypeWriteCompatible(e.KeyType, n.KeyType) &&
			isDatatypeWriteCompatible(e.ValueType, n.ValueType)
	}
	return _existingType.Name() == _newType.Name()
}

func isStructWriteCompatible(_existingSchema *StructType, _newSchema *StructType) bool {

	existing := toFieldMap(_existingSchema.GetFields())
	existingFieldNames := mapset.NewSet(fp.Map(func(s string) string { return strings.ToLower(s) })(_existingSchema.FieldNames())...)
	if existingFieldNames.Cardinality() != _existingSchema.Length() {
		panic("Delta tables don't allow field names that only differ by case")
	}
	newFields := mapset.NewSet(fp.Map(func(s string) string { return strings.ToLower(s) })(_newSchema.FieldNames())...)
	if newFields.Cardinality() != _newSchema.Length() {
		panic("Delta tables don't allow field names that only differ by case")
	}

	if !existingFieldNames.IsSubset(newFields) {
		return false
	}

	isCompatible := true
	for _, newField := range _newSchema.GetFields() {
		if v := existing.get(newField.Name); v.IsPresent() {
			existingField := v.MustGet()
			isCompatible = isCompatible && (existingField.Name == newField.Name) &&
				(!existingField.Nullable || newField.Nullable) &&
				(isDatatypeWriteCompatible(existingField.DataType, newField.DataType))
		}
	}
	return isCompatible
}

// As the Delta table updates, the schema may change as well.
// This method defines whether a new schema can replace a pre-existing schema of a Delta table.
// Our rules are to return false if the new schema:
// - Drops any column that is present in the current schema
// - Converts nullable=true to nullable=false for any column
// - Changes any datatype
func IsWriteCompatible(existingSchema *StructType, newSchema *StructType) bool {
	return isStructWriteCompatible(existingSchema, newSchema)
}

func toFieldMap(fields []*StructField) *caseInsensitiveMap {
	// note:  we simply lower the key, but Scala version creates a dedicated map called 'CaseInsensitiveMap'
	m := make(map[string]*StructField, len(fields))
	for _, field := range fields {
		m[strings.ToLower(field.Name)] = field
	}
	return &caseInsensitiveMap{m: m}
}

type caseInsensitiveMap struct {
	m map[string]*StructField
}

func (c *caseInsensitiveMap) get(key string) mo.Option[*StructField] {
	lowerKey := strings.ToLower(key)
	v, ok := c.m[lowerKey]
	return mo.TupleToOption(v, ok)
}

func matchesNullableType(x DataType) bool {
	switch dt := x.(type) {

	case *StructType:
		s := dt
		for _, field := range s.Fields {
			if !(field.Nullable && matchesNullableType(field.DataType)) {
				return false
			}
		}
		return true

	case *ArrayType:
		a := dt
		switch eleType := a.ElementType.(type) {
		case *StructType:
			s := eleType
			return a.ContainsNull && matchesNullableType(s)
		default:
			return a.ContainsNull
		}

	case *MapType:
		m := dt
		if Is[*StructType](m.KeyType) && Is[*StructType](m.ValueType) {
			return m.ValueContainsNull && matchesNullableType(m.KeyType) && matchesNullableType(m.ValueType)
		} else if Is[*StructType](m.KeyType) {
			return m.ValueContainsNull && matchesNullableType(m.KeyType)
		} else if Is[*StructType](m.ValueType) {
			return m.ValueContainsNull && matchesNullableType(m.ValueType)
		} else {
			return true
		}

	default:
		return true
	}
}

func prettyFieldName(columnPath []string) string {
	fn := func(s string) string {
		if strings.Contains(s, ".") {
			return "$" + s
		}
		return s
	}
	cols := fp.Map(fn)(columnPath)
	return strings.Join(cols, ".")
}

var invalidParquetChars *regexp.Regexp = regexp.MustCompile(".*[ ,;{}()\n\t=].*")
var parquetSchemaConverterObj *parquetSchemaConverter = &parquetSchemaConverter{}

type parquetSchemaConverter struct{}

func (p *parquetSchemaConverter) checkFieldName(name string) error {
	if invalidParquetChars.MatchString(name) {
		return &errno.DeltaStandaloneError{
			Msg: fmt.Sprintf(`Attribute name "%s" contains invalid character(s) among " ,;{}()\\n\\t=".
				 |Please use alias to rename it.`, name),
		}
	}
	return nil
}

func (p *parquetSchemaConverter) checkFieldNames(names []string) error {

	var msg string
	for _, name := range names {
		err := p.checkFieldName(name)
		if err != nil {
			msg += err.Error()
		}
	}
	if len(msg) != 0 {
		return &errno.DeltaStandaloneError{
			Msg: msg,
		}
	}
	return nil
}

func explode(schema *StructType) [][]string {
	return fp.FlatMap(func(f *StructField) [][]string {
		name := f.Name
		head := [][]string{{name}}

		mapper := func(nested []string) []string { return append([]string{name}, nested...) }

		switch v := f.DataType.(type) {
		case *StructType:
			return append(head, fp.Map(mapper)(explode(v))...)
		case *ArrayType:
			return append(head, fp.Map(mapper)(recurseIntoComplexTypes(v))...)
		case *MapType:
			return append(head, fp.Map(mapper)(recurseIntoComplexTypes(v))...)
		default:
			return head
		}
	})(schema.GetFields())

}

func recurseIntoComplexTypes(complexType DataType) [][]string {
	switch v := complexType.(type) {
	case *StructType:
		return explode(v)
	case *ArrayType:
		mapper := func(s []string) []string { return append([]string{"element"}, s...) }
		return fp.Map(mapper)(recurseIntoComplexTypes(v.ElementType))
	case *MapType:
		keyMapper := func(s []string) []string { return append([]string{"key"}, s...) }
		valMapper := func(s []string) []string { return append([]string{"value"}, s...) }
		l1 := fp.Map(keyMapper)(recurseIntoComplexTypes(v.KeyType))
		l2 := fp.Map(valMapper)(recurseIntoComplexTypes(v.ValueType))
		return append(l1, l2...)
	default:
		return nil
	}
}

func ExplodeNestedFieldNames(schema *StructType) []string {
	mapper := func(nameParts []string) string {
		parts := fp.Map(func(n string) string {
			if strings.Contains(n, ".") {
				return fmt.Sprintf("`%s`", n)
			} else {
				return n
			}
		})(nameParts)
		return strings.Join(parts, ".")
	}

	return fp.Map(mapper)(explode(schema))
}

func CheckColumnNameDuplication(schema *StructType, colType string) error {
	columnNames := ExplodeNestedFieldNames(schema)
	names := fp.Map(func(s string) string { return strings.ToLower(s) })(columnNames)

	if mapset.NewSet(names...).Cardinality() != len(names) {
		return &errno.DeltaStandaloneError{
			Msg: fmt.Sprintf("Found duplicated columns %s", colType),
		}
	}
	return nil
}
