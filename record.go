package deltago

import (
	"fmt"
	"strconv"
	"time"

	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/types"
	"github.com/rotisserie/eris"
	"github.com/shopspring/decimal"
)

type PartitionRowRecord struct {
	partitionSchema *types.StructType
	partitionValues map[string]string
}

func (r *PartitionRowRecord) getPrimitive(field *types.StructField) (string, error) {
	partitionValue, ok := r.partitionValues[field.Name]
	if !ok || len(partitionValue) == 0 {
		return "", eris.Wrap(errno.NullValueFoundForPrimitiveTypes(field.Name), "")
	}
	return partitionValue, nil
}

func (r *PartitionRowRecord) getNonPrimitive(field *types.StructField) (string, error) {
	partitionValue, ok := r.partitionValues[field.Name]
	if !ok || len(partitionValue) == 0 {
		if !field.Nullable {
			return "", eris.Wrap(errno.NullValueFoundForNonNullSchemaField(field.Name, fmt.Sprintf("%+v", r.partitionSchema)), "")
		}
	}
	return partitionValue, nil
}

func (p *PartitionRowRecord) Schema() types.StructType {
	return *p.partitionSchema
}

func (p *PartitionRowRecord) Length() int {
	return len(p.partitionSchema.Fields)
}

func (p *PartitionRowRecord) IsNullAt(fieldName string) (bool, error) {
	if _, err := p.partitionSchema.Get(fieldName); err != nil {
		return false, err
	}

	isNull := true
	if v, exist := p.partitionValues[fieldName]; exist {
		isNull = len(v) == 0
	}

	return isNull, nil
}

func (p *PartitionRowRecord) GetInt(fieldName string) (int, error) {
	v, err := checkPrimitiveField[*types.IntegerType](p, fieldName, "interger")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(v)
}

func (p *PartitionRowRecord) GetInt64(fieldName string) (int64, error) {
	v, err := checkPrimitiveField[*types.LongType](p, fieldName, "long")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

func (p *PartitionRowRecord) GetByte(fieldName string) (int8, error) {
	// in GO, byte is uint8, but in Java, byte is int8
	s, err := checkPrimitiveField[*types.ByteType](p, fieldName, "byte")
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(s, 10, 8)
	if err != nil {
		return 0, err
	}
	return int8(v), nil
}

func (p *PartitionRowRecord) GetShort(fieldName string) (int16, error) {
	s, err := checkPrimitiveField[*types.ShortType](p, fieldName, "short")
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

func (p *PartitionRowRecord) GetBoolean(fieldName string) (bool, error) {
	s, err := checkPrimitiveField[*types.BooleanType](p, fieldName, "bool")
	if err != nil {
		return false, err
	}
	return strconv.ParseBool(s)
}

func (p *PartitionRowRecord) GetFloat(fieldName string) (float32, error) {
	s, err := checkPrimitiveField[*types.FloatType](p, fieldName, "float")
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(s, 32)
	return float32(v), err
}

func (p *PartitionRowRecord) GetDouble(fieldName string) (float64, error) {
	s, err := checkPrimitiveField[*types.DoubleType](p, fieldName, "double")
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(s, 64)
}

func (p *PartitionRowRecord) GetString(fieldName string) (string, error) {
	return checkPrimitiveField[*types.StringType](p, fieldName, "string")
}

func (p *PartitionRowRecord) GetBinary(fieldName string) ([]byte, error) {
	s, err := checkPrimitiveField[*types.BinaryType](p, fieldName, "binary")
	if err != nil {
		return nil, err
	}
	return []byte(s), err
}

func (p *PartitionRowRecord) GetBigDecimal(fieldName string) (decimal.Decimal, error) {
	s, err := checkPrimitiveField[*types.BinaryType](p, fieldName, "decimal")
	if err != nil {
		return decimal.Decimal{}, err
	}
	return decimal.NewFromString(s)
}

func (p *PartitionRowRecord) GetTimestamp(fieldName string) (time.Time, error) {
	s, err := checkPrimitiveField[*types.TimestampType](p, fieldName, "timestamp")
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse("2021-09-08 11:11:11", s)
}

func (p *PartitionRowRecord) GetDate(fieldName string) (time.Time, error) {
	s, err := checkPrimitiveField[*types.DateType](p, fieldName, "date")
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse("2006-01-02", s)
}

func (p *PartitionRowRecord) GetRecord(fieldName string) (types.RowRecord, error) {
	return nil, eris.Wrap(errno.ErrUnsupportedOperation, "Struct is not a supported partition type")
}

func (p *PartitionRowRecord) GetList(fieldName string) ([]any, error) {
	return nil, eris.Wrap(errno.ErrUnsupportedOperation, "Array is not a supported partition type")
}

func (p *PartitionRowRecord) GetMap(fieldName string) (map[any]any, error) {
	return nil, eris.Wrap(errno.ErrUnsupportedOperation, "Map is not a supported partition type")
}

func checkPrimitiveField[T types.DataType](p *PartitionRowRecord, fieldName string, expectedType string) (string, error) {
	f, err := p.partitionSchema.Get(fieldName)
	if err != nil {
		return "", err
	}
	if !types.Is[T](f.DataType) {
		return "", errno.FieldTypeMismatch(fieldName, f.DataType.Name(), expectedType)
	}

	return p.getPrimitive(f)
}
