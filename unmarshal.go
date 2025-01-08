package dbi

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type taggroup struct {
	Field   string
	Column  string
	Options map[string]string
}

// Unmarshal rows into a slice with pointers to a struct. The mapping
// between row columns and struct fields are done with field tags
// named dbi. When getting dates from the database, the pq modules
// returnes a date format looking very much like a timestamp including
// time and time zone. To get rid of this, the unmarshaler can strip
// away the time part if the field type is a string and the option date
// is specified after the column name. Example. `dbi:"date,date"`
func (c *Config) Unmarshal(v interface{}, SQL string, args ...interface{}) error {
	return c.unmarshal(nil, v, SQL, args...)
}

// UnmarshalReadOnly rows into a slice with pointers to a struct. The
// mapping between row columns and struct fields are done with field
// tags named dbi. When getting dates from the database, the pq
// modules returnes a date format looking very much like a timestamp
// including time and time zone. To get rid of this, the unmarshaler
// can strip away the time part if the field type is a string and the
// option date is specified after the column
// name. Example. `dbi:"date,date"`
func (c *Config) UnmarshalReadOnly(v interface{}, SQL string, args ...interface{}) error {
	return c.unmarshal(&sql.TxOptions{ReadOnly: true}, v, SQL, args...)
}

// UnmarshalWithOptions rows into a slice with pointers to a struct. The
// mapping between row columns and struct fields are done with field
// tags named dbi. When getting dates from the database, the pq
// modules returnes a date format looking very much like a timestamp
// including time and time zone. To get rid of this, the unmarshaler
// can strip away the time part if the field type is a string and the
// option date is specified after the column
// name. Example. `dbi:"date,date"`
func (c *Config) UnmarshalWithOptions(txOpts *sql.TxOptions, v interface{}, SQL string, args ...interface{}) error {
	return c.unmarshal(txOpts, v, SQL, args...)
}

func (c *Config) unmarshal(txOpts *sql.TxOptions, v interface{}, SQL string, args ...interface{}) error {
	var targetSlice reflect.Value
	var t reflect.Type

	// Verify the pointer to a splice of struct pointers
	if reflect.TypeOf(v).Kind() == reflect.Ptr &&
		reflect.TypeOf(v).Elem().Kind() == reflect.Slice &&
		reflect.TypeOf(v).Elem().Elem().Kind() == reflect.Ptr &&
		reflect.TypeOf(v).Elem().Elem().Elem().Kind() == reflect.Struct {
		targetSlice = reflect.ValueOf(v).Elem()
		t = reflect.TypeOf(v).Elem().Elem().Elem()
	} else {
		return fmt.Errorf("dbi: argument must be a ponter to a slice with pointers to a struct")
	}

	// Check that we can write to the slice
	if !targetSlice.CanSet() {
		return fmt.Errorf("dbi: unable to set/change slice")
	}

	// Collect the fields/columns we are going to populate
	populate := make(map[string]taggroup)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Tag == "" {
			continue
		}
		if tag, ok := f.Tag.Lookup("dbi"); ok {
			// Column and options are separted with commas. First "option" is the column name
			options := strings.Split(tag, ",")
			if len(options) > 0 && options[0] != "" {
				tag := taggroup{Field: f.Name,
					Column:  options[0],
					Options: make(map[string]string)}
				for _, option := range options[1:] {
					tag.Options[option] = ""
				}

				if _, ok := populate[tag.Column]; ok {
					return fmt.Errorf("dbi: column %s used on multiple fields", tag.Column)
				}
				populate[tag.Column] = tag
			}
		}
	}

	// Convert ? to $1 and $2 etc.
	if c.Driver == "postgres" {
		SQL = postgresPlaceholders(SQL)
	}

	tx, err := c.db.BeginTx(context.Background(), txOpts)
	if err != nil {
		return fmt.Errorf("dbi: begin: %v", err)
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

	rows, err := tx.Query(SQL, args...)
	if err != nil {
		return fmt.Errorf("dbi: query: %v", err)
	}

	defer rows.Close()

	cols, _ := rows.Columns()

	colCheck := make(map[string]bool)
	for _, c := range cols {
		colCheck[c] = true
	}
	for c := range populate {
		if _, ok := colCheck[c]; !ok {
			return fmt.Errorf("dbi: query didn't return any columns with name %s", c)
		}
	}

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		n := reflect.New(t)
		newe := n.Elem()
		t = newe.Type()
		var dateOptions []int
		var aggOptions []int
	colLoop:
		for i := range columns {
			if p, ok := populate[cols[i]]; ok {
				for j := 0; j < t.NumField(); j++ {
					if t.Field(j).Name == p.Field &&
						newe.Field(j).CanSet() &&
						newe.Field(j).CanAddr() {
						addr := newe.Field(j).Addr()
						if addr.CanInterface() {
							columnPointers[i] = addr.Interface()
							if len(p.Options) > 0 {
								if _, ok := p.Options["agg"]; ok {
									columnPointers[i] = &columns[i]
									aggOptions = append(aggOptions, j)
								}
								if _, ok := p.Options["date"]; ok && t.Field(j).Type.Name() == "string" {
									dateOptions = append(dateOptions, j)
								}
							}

							continue colLoop
						}
					}
				}
			}
			// Discard column
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return fmt.Errorf("dbi: row scan: %v", err)
		}

		for _, i := range dateOptions {
			if newe.Field(i).CanSet() {
				str := newe.Field(i).String()
				if len(str) > 10 {
					newe.Field(i).Set(reflect.ValueOf(str[0:10]))
				}
			}
		}

		for _, i := range aggOptions {
			if newe.Field(i).CanSet() {
				str := newe.Field(i).String()
				if len(str) > 10 {
					newe.Field(i).Set(reflect.ValueOf(str[0:10]))
				}
			}
		}

		targetSlice.Set(reflect.Append(targetSlice, n))
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("dbi: rows: %v", err)
	}

	return err
}
