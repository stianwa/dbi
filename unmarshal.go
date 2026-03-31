package dbi

import (
	"fmt"
	"reflect"
)

// Unmarshal executes query within a transaction and scans all rows into v.
//
// v must be a pointer to a slice of pointers to structs. Each row is mapped
// to a new struct instance based on column names and struct metadata.
//
// If configured, SET LOCAL statements are applied before executing the query.
// On success, the transaction is committed; otherwise it is rolled back.
//
// For PostgreSQL, '?' placeholders are rewritten to $1, $2, ... before execution.
//
// # An error is returned if validation, query execution, scanning, or commit fails
//
// The `dbi` tag defines how a struct field maps to a query column and how
// the scanned value is processed.
//
// The tag format is:
//
//	`dbi:"column[,option[,option...]]"`
//
// The first value specifies the column name expected in the query result.
// Fields without a `dbi` tag are ignored.
//
// All tagged columns must be present in the query result. If a column defined
// in a tag is missing, Unmarshal returns an error.
//
// Supported options:
//
//   - date
//     If the field is of type string, the scanned value is truncated to its
//     first 10 characters after assignment. No effect on non-string fields.
//
//   - zeronull
//     The column is scanned into an intermediate value. If the database value
//     is NULL, the field is set to its zero value. Otherwise, the value is
//     assigned using the package's conversion logic (assignRawToField).
//     This option must not be used on pointer fields.
//
// Notes:
//
//   - Fields with zeronull are always scanned via an intermediate value.
//   - Other fields are scanned directly into the struct field when possible.
//   - If a field implements sql.Scanner, it is used during assignment when
//     scanning via intermediate values.
//   - Basic type conversions are attempted for numeric, []byte, and time.Time values.
//
// Example:
//
//	type row struct {
//	    ID        int       `dbi:"id"`
//	    Name      string    `dbi:"name"`
//	    CreatedAt string    `dbi:"created_at,date"`
//	    Count     int       `dbi:"count,zeronull"`
//	}
func (q *QueryOptions) Unmarshal(v any, query string, args ...any) (err error) {
	targetSlice, structType, err := validateTargetSlice(v)
	if err != nil {
		return err
	}

	meta, err := getStructMetadata(structType)
	if err != nil {
		return err
	}

	if q.driver == "postgres" {
		query = postgresPlaceholders(query)
	}

	tx, err := q.db.BeginTx(q.ctx, q.txOptions)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
		if q.cancel != nil {
			q.cancel()
		}
	}()

	if q.setConfig != nil {
		for _, s := range q.setConfig.queries() {
			_, err := tx.Exec(s.SQL, s.Value)
			if err != nil {
				return err
			}
		}
	}

	rows, err := tx.QueryContext(q.ctx, query, args...)
	if err != nil {
		return wrapQueryErr(q.ctx, err, "query")
	}

	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("columns: %w", err)
	}

	colBindings, err := buildColumnBindings(cols, meta)
	if err != nil {
		return err
	}

	for rows.Next() {
		n := reflect.New(structType)
		elem := n.Elem()

		if err := scanOneRow(rows, elem, colBindings); err != nil {
			return err
		}

		targetSlice.Set(reflect.Append(targetSlice, n))
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	committed = true
	return nil
}
