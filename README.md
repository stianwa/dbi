# dbi
[![Go Reference](https://pkg.go.dev/badge/github.com/stianwa/dbi.svg)](https://pkg.go.dev/github.com/stianwa/dbi) [![Go Report Card](https://goreportcard.com/badge/github.com/stianwa/dbi)](https://goreportcard.com/report/github.com/stianwa/dbi)

Package dbi implements a database/sql wrapper.

This is an EXPERIMENTAL package used for experimenting.

Installation
------------

The recommended way to install dbi

```
go get github.com/stianwa/dbi
```

Examples
--------

```go

package main

import (
       "github.com/stianwa/dbi"
     _ "github.com/lib/pq"
       "fmt"
	   "time"
)

type SomeData struct {
       Name string `dbi:"name"`
       Age  int    `dbi:"age"`
}

func main() {
       db := &dbi.Config{Name: "db",
                         User: "dbuser}

       if err := db.Open(); err != nil {
          fmt.Printf("%v\n", err)
          return
       }
	   defer db.Close()

       var rows []*SomeData

	   q := db.NewQueryOptions(dbi.WithTimeout(5 * time.Second))
	   defer q.Cancel()

       if err := q.Unmarshal(&rows, "select * from some_table where age = ?", 21); err != nil {
          fmt.Printf("%v\n", err)
          return
       }

       for _, row := range rows {
          fmt.Printf("name: %s age: %d\n", row.Name, row.Age)
       }
}
```

State
-------
The dbi module is experimental and under development. Do not use for production.


License
-------

MIT, see [LICENSE.md](LICENSE.md)
