package column

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/kshvakov/clickhouse/lib/binary"
)

type ArrayWriter interface {
	WriteArray(encoder *binary.Encoder, column Column) (uint64, error)
}

type Array struct {
	base
	column Column
}

func (array *Array) Read(decoder *binary.Decoder) (interface{}, error) {
	return nil, fmt.Errorf("do not use Read method for Array(T) column")
}

func (array *Array) Write(encoder *binary.Encoder, v interface{}) error {
	return fmt.Errorf("do not use Write method for Array(T) column")
}

func (array *Array) ReadArray(decoder *binary.Decoder, ln int) (interface{}, error) {
	slice := reflect.MakeSlice(array.valueOf.Type(), 0, ln)
	for i := 0; i < ln; i++ {
		value, err := array.column.Read(decoder)
		if err != nil {
			return nil, err
		}
		slice = reflect.Append(slice, reflect.ValueOf(value))
	}
	return slice.Interface(), nil
}

func (array *Array) WriteArray(encoder *binary.Encoder, v interface{}) (uint64, error) {
	switch value := v.(type) {
	case ArrayWriter:
		return value.WriteArray(encoder, array.column)
	case []byte:
		var (
			buff    = bytes.NewBuffer(value)
			decoder = binary.NewDecoder(buff)
		)
		ln, err := decoder.Uvarint()
		if err != nil {
			return 0, err
		}
		switch array.column.(type) {
		case *Enum:
			slice := make([]string, 0, ln)
			for i := 0; i < int(ln); i++ {
				v, err := decoder.String()
				if err != nil {
					return 0, err
				}
				slice = append(slice, v)
			}
			for _, v := range slice {
				if err := array.column.Write(encoder, v); err != nil {
					return 0, err
				}
			}
		default:
			if _, err := buff.WriteTo(encoder); err != nil {
				return 0, err
			}
		}
		return ln, nil
	}
	return 0, nil
}

func parseArray(name, chType string, timezone *time.Location) (*Array, error) {
	if len(chType) < 11 {
		return nil, fmt.Errorf("invalid Array column type: %s", chType)
	}
	column, err := Factory(name, chType[6:][:len(chType)-7], timezone)
	if err != nil {
		return nil, fmt.Errorf("Array(T): %v", err)
	}

	var scanType interface{}
	switch t := column.ScanType().Kind(); t {
	case reflect.Int8:
		scanType = []int8{}
	case reflect.Int16:
		scanType = []int16{}
	case reflect.Int32:
		scanType = []int32{}
	case reflect.Int64:
		scanType = []int64{}
	case reflect.Uint8:
		scanType = []uint8{}
	case reflect.Uint16:
		scanType = []uint16{}
	case reflect.Uint32:
		scanType = []uint32{}
	case reflect.Uint64:
		scanType = []uint64{}
	case reflect.Float32:
		scanType = []float32{}
	case reflect.Float64:
		scanType = []float64{}
	case reflect.String:
		scanType = []string{}
	case baseTypes[time.Time{}].Kind():
		scanType = []time.Time{}
	default:
		return nil, fmt.Errorf("unsupported array type '%s'", column.ScanType().Name())
	}
	return &Array{
		base: base{
			name:    name,
			chType:  chType,
			valueOf: reflect.ValueOf(scanType),
		},
		column: column,
	}, nil
}
