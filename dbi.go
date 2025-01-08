// Package dbi provides library methods for a sql/pg wrapper.
package dbi

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Config holds the connection string parameters and handler
type Config struct {
	// Database name
	Name string `yaml:"name"     json:"name"`
	// Database user name
	User string `yaml:"user"     json:"user"`
	// Database password can be one of:
	// 1) password text
	// 2) pointer to a file containing the password:       file:/path/to/secret.pw
	// 3) an environment variable containing the password: env:PASSWORD_ENV
	Password string `yaml:"password" json:"password"`
	// SSLMode: disable,allow,prefer,require,verify-ca,verify-full. Default: disable
	SSLMode string `yaml:"sslmode"  json:"sslmode"`
	// Driver, only postgresql (the default) is tested.
	Driver string `yaml:"driver"   json:"driver"`
	// Hostname can be a hostname or a path to a socket. Defaults to /var/run/postgresql
	Host string `yaml:"host"     json:"host"`
	// Hostname can be a hostname or a path to a socket. Defaults to /var/run/postgresql
	Port int `yaml:"port"     json:"port"`
	// Maximum open connections
	MaxOpenConns int `yaml:"maxOpenConns"`
	// Maximum idle connections
	MaxIdleConns int     `yaml:"maxIdleConns"`
	db           *sql.DB `yaml:"-"        json:"-"`
}

// Open initialize a configuration. An error is returned if the
// initializing failed.
func (c *Config) Open() error {
	if c.User == "" {
		return fmt.Errorf("db user missing")
	}

	if c.Name == "" {
		return fmt.Errorf("db name missing")
	}

	if c.Driver == "" {
		c.Driver = "postgres"
	}
	if c.SSLMode == "" {
		c.SSLMode = "disable"
	}
	if c.Host == "" {
		c.Host = "/var/run/postgresql"
	}
	password := ""
	if c.Password != "" {
		if pw, err := getPassword(c.Password); err != nil {
			return err
		} else if pw != "" {
			password = "password=" + pw
		}
	}
	port := ""
	if c.Port != 0 {
		port = fmt.Sprintf(" port=%d", c.Port)
	}

	dbstr := fmt.Sprintf("user=%s dbname=%s %s sslmode=%s host=%s%s",
		c.User, c.Name, password, c.SSLMode, c.Host, port)
	db, err := sql.Open(c.Driver, dbstr)
	if err != nil {
		return err
	}
	c.db = db

	if err := c.db.Ping(); err != nil {
		return err
	}

	if c.MaxOpenConns > 0 {
		c.db.SetMaxOpenConns(c.MaxOpenConns)
	}

	if c.MaxIdleConns > 0 {
		c.db.SetMaxIdleConns(c.MaxIdleConns)
	}

	return nil
}

// Close the handler. Return error upon errors.
func (c *Config) Close() error {
	return c.db.Close()
}

// Ping the database. Return error upon errors.
func (c *Config) Ping() error {
	return c.db.Ping()
}

// PrepareTest test a sql, and returns an error if it fails.
func (c *Config) PrepareTest(sql string) error {
	_, err := c.db.Prepare(sql)
	return err
}

// postgresPlaceholder replaces the '?' placeholder with the $<n>
// placeholder used by Postgresql.
func postgresPlaceholders(sql string) string {
	ret := sql

	placeholderRe := regexp.MustCompile(`\?`)
	elems := placeholderRe.Split(sql, -1)
	if len(elems) > 1 {
		ret = ""
		argno := 1
		for i, sqlpart := range elems {
			if i != 0 {
				ret += fmt.Sprintf("$%d", argno)
				argno++
			}
			ret += sqlpart
		}
	}

	return ret
}

func getPassword(password string) (string, error) {
	// Read password from a different file if password has form
	// file:<path>
	if strings.HasPrefix(password, "env:") {
		env := password[len("env:"):]
		if p := os.Getenv(env); p != "" {
			return p, nil
		}
		return "", fmt.Errorf("reading password from environment vaiable %s: string empty", env)
	} else if strings.HasPrefix(password, "file:") {
		file := password[len("file:"):]
		data, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("reading password file %s: %v", file, err)
		}

		for len(data) > 0 && (data[len(data)-1] == '\n' || data[len(data)-1] == '\r') {
			data = data[0 : len(data)-1]
		}

		password = string(data)
	}

	return password, nil
}
