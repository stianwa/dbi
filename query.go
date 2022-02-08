package dbi

import (
	"database/sql"
	"fmt"
	"time"
)

// QueryInterface takes an SQL and data, and returns rows with string
// map to interfaces values.
func (c *Config) QueryInterface(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := c.MultiQuery([]string{sql}, args)
	if err != nil {
		return nil, err
	}

	var n []map[string]interface{}
	for _, row := range rows[0] {
		l := make(map[string]interface{})
		for k, v := range row {
			l[k] = v
		}
		n = append(n, l)
	}

	return n, nil
}

// QueryString takes an SQL and data, and returns rows with string map
// to stringified values.
func (c *Config) QueryString(sql string, args ...interface{}) ([]map[string]string, error) {
	rows, err := c.MultiQuery([]string{sql}, args)
	if err != nil {
		return nil, err
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
					str = fmt.Sprintf("%s", t.Format(time.RFC3339Nano))
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

// Query takes an SQL and data, and returns rows with string map to
// interfaces values.
func (c *Config) Query(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := c.MultiQuery([]string{sql}, args)
	if err != nil {
		return nil, err
	}

	return rows[0], nil
}

// MultiQuery takes a SQL list and data list (paired), and returns set
// of rows with string map to interfaces values.
func (c *Config) MultiQuery(sql []string, args ...[]interface{}) ([][]map[string]interface{}, error) {
	var ret [][]map[string]interface{}

	tx, err := c.db.Begin()
	if err != nil {
		return nil, err
	}

	for len(sql) > len(args) {
		args = append(args, []interface{}{})
	}

	defer func() {
		if p := recover(); p != nil {
			// a panic occurred, rollback and repanic
			tx.Rollback()
			panic(p)
		} else if err != nil {
			// something went wrong, rollback
			tx.Rollback()
		} else {
			// all good, commit
			err = tx.Commit()
		}
	}()

	for i, s := range sql {
		if c.Driver == "postgres" {
			s = postgresPlaceholders(s)
		}

		rows, err := tx.Query(s, args[i]...)
		if err != nil {
			return nil, err
		}

		defer rows.Close()
		cols, _ := rows.Columns()
		var rowset []map[string]interface{}
		for rows.Next() {
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i := range columns {
				columnPointers[i] = &columns[i]
			}

			// Scan the result into the column pointers...
			if err := rows.Scan(columnPointers...); err != nil {
				return nil, err
			}

			// Create our map, and retrieve the value for each column from the pointers slice,
			// storing it in the map with the name of the column as the key.
			m := make(map[string]interface{})
			for i, colName := range cols {
				val := columnPointers[i].(*interface{})
				m[colName] = *val
			}

			// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
			rowset = append(rowset, m)
		}
		err = rows.Err()
		if err != nil {
			return nil, err
		}
		ret = append(ret, rowset)
	}

	return ret, err
}

// Upsert takes a SQL and an array list of values for upserts.
// of rows with string map to interfaces values.
func (c *Config) Upsert(sql string, array [][]interface{}) error {
	if c.Driver == "postgres" {
		sql = postgresPlaceholders(sql)
	}

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			// a panic occurred, rollback and repanic
			tx.Rollback()
			panic(p)
		} else if err != nil {
			// something went wrong, rollback
			tx.Rollback()
		} else {
			// all good, commit
			err = tx.Commit()
		}
	}()

	stmt, err := tx.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i, a := range array {
		if _, err := stmt.Exec(a...); err != nil {
			return fmt.Errorf("upsert on row %d failed: %v", i, err)
		}
	}

	return err
}

// Transaction takes a SQL list and an array list of values for transactions.
// of rows with string map to interfaces values.
func (c *Config) Transaction(sqls []string, args ...[][]interface{}) error {
	for len(sqls) != len(args) {
		return fmt.Errorf("uneven set of sql and data")
	}

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			// a panic occurred, rollback and repanic
			tx.Rollback()
			panic(p)
		} else if err != nil {
			// something went wrong, rollback
			tx.Rollback()
		} else {
			// all good, commit
			err = tx.Commit()
		}
	}()

	var stmtlist []*sql.Stmt
	for _, s := range sqls {
		if c.Driver == "postgres" {
			s = postgresPlaceholders(s)
		}
		stmt, err := tx.Prepare(s)
		if err != nil {
			return err
		}
		defer stmt.Close()
		stmtlist = append(stmtlist, stmt)
	}

	for setno := range sqls {
		for rowno, row := range args[setno] {
			if _, err := stmtlist[setno].Exec(row...); err != nil {
				return fmt.Errorf("sql set %d, row %d: %v", setno+1, rowno+1, err)
			}
		}
	}

	return nil
}
