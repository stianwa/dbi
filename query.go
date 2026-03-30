package dbi

import (
	"fmt"
	"time"
)

// QueryAny executes query and returns its rows as a slice of column-value maps.
func (q *Query) QueryAny(query string, args ...any) ([]map[string]any, error) {
	rows, err := q.MultiQuery([]string{query}, args)
	if err != nil {
		return nil, err
	}

	if len(rows) != 1 {
		return nil, fmt.Errorf("query returned %d rows, expected 1", len(rows))
	}

	return rows[0], nil
}

// QueryString executes query and returns its rows as a slice of column-value
// maps with stringified values.
func (q *Query) QueryString(query string, args ...any) ([]map[string]string, error) {
	rows, err := q.MultiQuery([]string{query}, args)
	if err != nil {
		return nil, err
	}

	if len(rows) != 1 {
		return nil, fmt.Errorf("query returned %d rows, expected 1", len(rows))
	}

	var n []map[string]string
	for _, row := range rows[0] {
		l := make(map[string]string)
		for k, v := range row {
			str := ""
			if v != nil {
				switch t := v.(type) {
				case string:
					str = v.(string)
				case int64:
					str = fmt.Sprintf("%d", v)
				case int32:
					str = fmt.Sprintf("%d", v)
				case int16:
					str = fmt.Sprintf("%d", v)
				case uint8:
					str = fmt.Sprintf("%c", v)
				case float32:
					str = fmt.Sprintf("%f", v)
				case float64:
					str = fmt.Sprintf("%f", v)
				case bool:
					str = fmt.Sprintf("%t", v)
				case time.Time:
					str = t.Format(time.RFC3339Nano)
				default:
					str = fmt.Sprintf("%s", v)
				}
			}
			l[k] = str
		}
		n = append(n, l)
	}

	return n, nil
}

// MultiQuery executes each query with its matching argument list and returns
// one result set per query.
//
// All queries are executed in a single transaction. If fewer argument lists
// than queries are provided, the remaining queries are executed with no
// arguments.
func (q *Query) MultiQuery(queries []string, argsList ...[]any) (ret [][]map[string]any, err error) {
	if len(argsList) > len(queries) {
		return nil, fmt.Errorf("dbi: got %d argument sets for %d queries", len(argsList), len(queries))
	}

	tx, err := q.db.BeginTx(q.ctx, q.txOptions)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
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

	if q.setLocal != nil {
		for _, s := range q.setLocal.queries() {
			_, err := tx.Exec(s.SQL, s.Value)
			if err != nil {
				return nil, err
			}
		}
	}

	ret = make([][]map[string]any, 0, len(queries))

	for i, query := range queries {
		if err := q.ctx.Err(); err != nil {
			return nil, err
		}

		if q.driver == "postgres" {
			query = postgresPlaceholders(query)
		}

		var queryArgs []any
		if i < len(argsList) {
			queryArgs = argsList[i]
		}

		rows, err := tx.QueryContext(q.ctx, query, queryArgs...)
		if err != nil {
			return nil, wrapQueryErr(q.ctx, err, fmt.Sprintf("query %d", i))
		}

		rowset, err := scanRowsToMaps(rows)
		_ = rows.Close()
		if err != nil {
			return nil, fmt.Errorf("query %d: %w", i, err)
		}

		ret = append(ret, rowset)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	committed = true
	return ret, nil
}
