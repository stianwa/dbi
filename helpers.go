package dbi

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

type structMetadata struct {
	structType reflect.Type
	byColumn   map[string]fieldBinding
}

type fieldBinding struct {
	fieldIndex int
	fieldName  string
	fieldType  reflect.Type
	date       bool
	zeronull   bool
}

type colBinding struct {
	columnName string
	binding    *fieldBinding
}

var structMetadataCache sync.Map // map[reflect.Type]*structMetadata

func validateTargetSlice(v any) (reflect.Value, reflect.Type, error) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return reflect.Value{}, nil, fmt.Errorf("dbi: argument must be a pointer to a slice of pointers to struct")
	}

	if rv.Kind() != reflect.Ptr {
		return reflect.Value{}, nil, fmt.Errorf("dbi: argument must be a pointer to a slice of pointers to struct")
	}

	sliceValue := rv.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return reflect.Value{}, nil, fmt.Errorf("dbi: argument must be a pointer to a slice of pointers to struct")
	}

	sliceElemType := sliceValue.Type().Elem()
	if sliceElemType.Kind() != reflect.Ptr || sliceElemType.Elem().Kind() != reflect.Struct {
		return reflect.Value{}, nil, fmt.Errorf("dbi: argument must be a pointer to a slice of pointers to struct")
	}

	if !sliceValue.CanSet() {
		return reflect.Value{}, nil, fmt.Errorf("dbi: unable to set/change slice")
	}

	return sliceValue, sliceElemType.Elem(), nil
}

func getStructMetadata(structType reflect.Type) (*structMetadata, error) {
	if cached, ok := structMetadataCache.Load(structType); ok {
		return cached.(*structMetadata), nil
	}

	meta, err := buildStructMetadata(structType)
	if err != nil {
		return nil, err
	}

	actual, _ := structMetadataCache.LoadOrStore(structType, meta)
	return actual.(*structMetadata), nil
}

func buildStructMetadata(structType reflect.Type) (*structMetadata, error) {
	meta := &structMetadata{
		structType: structType,
		byColumn:   make(map[string]fieldBinding, structType.NumField()),
	}

	for i := 0; i < structType.NumField(); i++ {
		f := structType.Field(i)

		tag, ok := f.Tag.Lookup("dbi")
		if !ok || tag == "" {
			continue
		}

		columnName, options := parseDBITag(tag)
		if columnName == "" {
			continue
		}

		if _, exists := meta.byColumn[columnName]; exists {
			return nil, fmt.Errorf("column %q used on multiple fields", columnName)
		}

		if options["zeronull"] && f.Type.Kind() == reflect.Ptr {
			return nil, fmt.Errorf("zeronull cannot be used on pointer field %s", f.Name)
		}

		meta.byColumn[columnName] = fieldBinding{
			fieldIndex: i,
			fieldName:  f.Name,
			fieldType:  f.Type,
			date:       options["date"],
			zeronull:   options["zeronull"],
		}
	}

	return meta, nil
}

func parseDBITag(tag string) (string, map[string]bool) {
	parts := strings.Split(tag, ",")
	if len(parts) == 0 {
		return "", nil
	}

	columnName := strings.TrimSpace(parts[0])
	options := make(map[string]bool, len(parts)-1)

	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		if part != "" {
			options[part] = true
		}
	}

	return columnName, options
}

func buildColumnBindings(cols []string, meta *structMetadata) ([]colBinding, error) {
	colBindings := make([]colBinding, len(cols))
	colSeen := make(map[string]bool, len(cols))

	for i, col := range cols {
		colSeen[col] = true

		var bindingPtr *fieldBinding
		if binding, ok := meta.byColumn[col]; ok {
			b := binding
			bindingPtr = &b
		}

		colBindings[i] = colBinding{
			columnName: col,
			binding:    bindingPtr,
		}
	}

	for col := range meta.byColumn {
		if !colSeen[col] {
			return nil, fmt.Errorf("dbi: query didn't return any column named %q", col)
		}
	}

	return colBindings, nil
}

func scanOneRow(rows *sql.Rows, elem reflect.Value, colBindings []colBinding) error {
	rawValues := make([]any, len(colBindings))
	scanTargets := make([]any, len(colBindings))

	for i, cb := range colBindings {
		if cb.binding == nil {
			scanTargets[i] = &rawValues[i]
			continue
		}

		field := elem.Field(cb.binding.fieldIndex)
		if !field.CanSet() || !field.CanAddr() {
			scanTargets[i] = &rawValues[i]
			continue
		}

		// zeronull need raw handling after Scan.
		if cb.binding.zeronull {
			scanTargets[i] = &rawValues[i]
			continue
		}

		scanTargets[i] = field.Addr().Interface()
	}

	if err := rows.Scan(scanTargets...); err != nil {
		return fmt.Errorf("dbi: row scan: %w", err)
	}

	for i, cb := range colBindings {
		if cb.binding == nil {
			continue
		}

		field := elem.Field(cb.binding.fieldIndex)

		if cb.binding.zeronull {
			if err := assignRawToField(field, rawValues[i], cb.binding.zeronull); err != nil {
				return fmt.Errorf(
					"dbi: assign column %q to field %s: %w",
					cb.columnName,
					cb.binding.fieldName,
					err,
				)
			}
		}

		if cb.binding.date && field.Kind() == reflect.String {
			s := field.String()
			if len(s) > 10 {
				field.SetString(s[:10])
			}
		}
	}

	return nil
}

func assignRawToField(field reflect.Value, raw any, zeronull bool) error {
	if !field.CanSet() {
		return nil
	}

	if raw == nil {
		if zeronull {
			field.Set(reflect.Zero(field.Type()))
		}
		return nil
	}

	if canUseScanner(field) {
		return scanIntoScanner(field, raw, zeronull)
	}

	fieldType := field.Type()
	rawValue := reflect.ValueOf(raw)

	if rawValue.Type().AssignableTo(fieldType) {
		field.Set(rawValue)
		return nil
	}

	if b, ok := raw.([]byte); ok {
		if err := assignBytesToField(field, b); err == nil {
			return nil
		}
	}

	if t, ok := raw.(time.Time); ok {
		if err := assignTimeToField(field, t); err == nil {
			return nil
		}
	}

	if converted, ok := normalizeInteger(raw); ok {
		if converted.Type().ConvertibleTo(fieldType) {
			field.Set(converted.Convert(fieldType))
			return nil
		}
	}

	if converted, ok := normalizeUnsignedInteger(raw); ok {
		if converted.Type().ConvertibleTo(fieldType) {
			field.Set(converted.Convert(fieldType))
			return nil
		}
	}

	if converted, ok := normalizeFloat(raw); ok {
		if converted.Type().ConvertibleTo(fieldType) {
			field.Set(converted.Convert(fieldType))
			return nil
		}
	}

	if rawValue.Type().ConvertibleTo(fieldType) {
		field.Set(rawValue.Convert(fieldType))
		return nil
	}

	return fmt.Errorf("cannot assign %T to %s", raw, fieldType.String())
}

func canUseScanner(field reflect.Value) bool {
	if !field.CanAddr() {
		return false
	}

	scannerType := reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	return field.Addr().Type().Implements(scannerType)
}

func scanIntoScanner(field reflect.Value, raw any, zeronull bool) error {
	scanner, ok := field.Addr().Interface().(sql.Scanner)
	if !ok {
		return fmt.Errorf("field does not implement sql.Scanner")
	}

	if raw == nil && zeronull {
		field.Set(reflect.Zero(field.Type()))
		return nil
	}

	return scanner.Scan(raw)
}

func assignBytesToField(field reflect.Value, b []byte) error {
	fieldType := field.Type()

	switch field.Kind() {
	case reflect.String:
		field.SetString(string(b))
		return nil
	case reflect.Slice:
		if fieldType.Elem().Kind() == reflect.Uint8 {
			buf := make([]byte, len(b))
			copy(buf, b)
			field.SetBytes(buf)
			return nil
		}
	}

	return fmt.Errorf("cannot assign []byte to %s", fieldType.String())
}

func assignTimeToField(field reflect.Value, t time.Time) error {
	fieldType := field.Type()

	if fieldType == reflect.TypeOf(time.Time{}) {
		field.Set(reflect.ValueOf(t))
		return nil
	}

	if field.Kind() == reflect.String {
		field.SetString(t.Format(time.RFC3339Nano))
		return nil
	}

	return fmt.Errorf("cannot assign time.Time to %s", fieldType.String())
}

func normalizeInteger(v any) (reflect.Value, bool) {
	switch n := v.(type) {
	case int:
		return reflect.ValueOf(int64(n)), true
	case int8:
		return reflect.ValueOf(int64(n)), true
	case int16:
		return reflect.ValueOf(int64(n)), true
	case int32:
		return reflect.ValueOf(int64(n)), true
	case int64:
		return reflect.ValueOf(n), true
	default:
		return reflect.Value{}, false
	}
}

func normalizeUnsignedInteger(v any) (reflect.Value, bool) {
	switch n := v.(type) {
	case uint:
		return reflect.ValueOf(uint64(n)), true
	case uint8:
		return reflect.ValueOf(uint64(n)), true
	case uint16:
		return reflect.ValueOf(uint64(n)), true
	case uint32:
		return reflect.ValueOf(uint64(n)), true
	case uint64:
		return reflect.ValueOf(n), true
	default:
		return reflect.Value{}, false
	}
}

func normalizeFloat(v any) (reflect.Value, bool) {
	switch n := v.(type) {
	case float32:
		return reflect.ValueOf(float64(n)), true
	case float64:
		return reflect.ValueOf(n), true
	default:
		return reflect.Value{}, false
	}
}

// scanRowsToMaps scans all rows into a slice of column-value maps.
func scanRowsToMaps(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	values := make([]any, len(cols))
	scanTargets := make([]any, len(cols))
	for i := range values {
		scanTargets[i] = &values[i]
	}

	rowset := make([]map[string]any, 0)

	for rows.Next() {
		for i := range values {
			values[i] = nil
		}

		if err := rows.Scan(scanTargets...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		row := make(map[string]any, len(cols))
		for i, colName := range cols {
			row[colName] = cloneSQLValue(values[i])
		}

		rowset = append(rowset, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	return rowset, nil
}

// cloneSQLValue returns a stable copy of driver values that must not be reused
// directly, such as []byte.
func cloneSQLValue(v any) any {
	switch x := v.(type) {
	case []byte:
		b := make([]byte, len(x))
		copy(b, x)
		return b
	default:
		return x
	}
}

func wrapQueryErr(ctx context.Context, err error, op string) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) {
		if cause := context.Cause(ctx); cause != nil {
			return fmt.Errorf("%s: %w", op, cause)
		}
	}

	return fmt.Errorf("%s: %w", op, err)
}
