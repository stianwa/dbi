// Package dbi provides library methods for a sql/pg wrapper.
package dbi

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config peresents the connection string parameters and handler
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
	// Port number. Defaults to 5432
	Port int `yaml:"port"     json:"port"`
	// Application name
	ApplicationName string `yaml:"applicationName" json:"application_name"`
	// Client encoding
	ClientEncoding string `yaml:"clientEncoding" json:"client_encoding"`
	// Connect timeout
	ConnectTimeout int `yaml:"connectTimeout" json:"connect_timeout"`
	// Statement timeout
	StatementTimeout int `yaml:"statementTimeout" json:"statement_timeout"`
	// Idle-intransaction session timeout
	IdleInTransactionSessionTimeout int `yaml:"idleInTransactionSessionTimeout" json:"idle_in_transaction_session_timeout"`
	// SSLRootCert
	SSLRootCert string `yaml:"sslRootCert" json:"ssl_root_cert"`
	// SSLCert
	SSLCert string `yaml:"sslCert" json:"ssl_cert"`
	// SSLKey
	SSLKey string `yaml:"sslKey" json:"sslKey"`
	// Search path
	SearchPath string `yaml:"searchPath" json:"searchPath"`
	// Timezone
	Timezone string `yaml:"timezone" json:"timezone"`
	// Maximum open connections
	MaxOpenConns int `yaml:"maxOpenConns" json:"max_open_conns"`
	// Maximum idle connections
	MaxIdleConns int `yaml:"maxIdleConns"`
	// Query timeout for default context
	DefaultQueryTimeout time.Duration `yaml:"defaultQueryTimeout" json:"default_query_timeout"`
	// Query ReadOnly for default TxOptions
	DefaultQueryReadOnly bool `yaml:"defaultQueryReadOnly" json:"default_query_read_only"`
	// Query Isolation for default TxOptions
	DefaultQueryIsolation sql.IsolationLevel `yaml:"defaultQueryIsolation" json:"default_query_isolation"`
	db                    *sql.DB            `yaml:"-" json:"-"`
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

	dsn, err := c.dsn()
	if err != nil {
		return err
	}

	db, err := sql.Open(c.Driver, dsn)
	if err != nil {
		return err
	}
	c.db = db

	timeout := c.DefaultQueryTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	ctx, cancel := context.WithTimeoutCause(context.Background(),
		timeout,
		fmt.Errorf("timed out after %s", timeout))
	defer cancel()

	if err := c.db.PingContext(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("db ping: %w", context.Cause(ctx))
		}

		return fmt.Errorf("db ping: %w", err)
	}

	if c.MaxOpenConns > 0 {
		c.db.SetMaxOpenConns(c.MaxOpenConns)
	}

	if c.MaxIdleConns > 0 {
		c.db.SetMaxIdleConns(c.MaxIdleConns)
	}

	return nil
}

func (c *Config) dsn() (string, error) {
	password := ""
	if c.Password != "" {
		pw, err := getPassword(c.Password)
		if err != nil {
			return "", err
		}
		password = pw
	}
	port := ""
	if c.Port != 0 {
		port = fmt.Sprintf(":%d", c.Port)
	}

	u := url.URL{
		Scheme: c.Driver,
		User:   url.UserPassword(c.User, password),
		Host:   c.Host + port,
		Path:   c.Name,
	}

	q := u.Query()
	q.Set("sslmode", c.SSLMode)
	if c.ApplicationName != "" {
		q.Set("application_name", c.ApplicationName)
	}

	if c.ClientEncoding != "" {
		q.Set("client_encoding", c.ClientEncoding)
	}

	if c.ConnectTimeout != 0 {
		q.Set("connect_timeout", strconv.Itoa(c.ConnectTimeout))
	}

	if c.StatementTimeout != 0 {
		q.Set("statement_timeout", strconv.Itoa(c.StatementTimeout))
	}

	if c.IdleInTransactionSessionTimeout != 0 {
		q.Set("idle_in_transaction_session_timeout", strconv.Itoa(c.IdleInTransactionSessionTimeout))
	}

	if c.SSLRootCert != "" {
		q.Set("sslrootcert", c.SSLRootCert)
	}

	if c.SSLCert != "" {
		q.Set("sslcert", c.SSLCert)
	}

	if c.SSLKey != "" {
		q.Set("sslkey", c.SSLKey)
	}

	if c.SearchPath != "" {
		q.Set("search_path", c.SearchPath)
	}

	if c.Timezone != "" {
		q.Set("timezone", c.Timezone)
	}

	u.RawQuery = q.Encode()

	return u.String(), nil
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
func postgresPlaceholders(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 20) // small headroom

	arg := 1
	for i := 0; i < len(s); i++ {
		if s[i] == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(arg))
			arg++
		} else {
			b.WriteByte(s[i])
		}
	}

	return b.String()
}

// getPassword resolves a password value from a literal, environment variable,
// or file reference.
//
// The input may be prefixed to indicate the source:
//
//   - "env:NAME"
//     Reads the password from the environment variable NAME. Returns an error
//     if the variable is not set or is empty.
//
//   - "file:/path/to/file"
//     Reads the password from the given file. The file contents are trimmed
//     with strings.TrimSpace before returning.
//
// If no recognized prefix is present, the input string is returned as-is.
func getPassword(password string) (string, error) {
	if env, ok := strings.CutPrefix(password, "env:"); ok {
		if p := os.Getenv(env); p != "" {
			return p, nil
		}
		return "", fmt.Errorf("reading password from environment variable %s: string empty", env)
	} else if file, ok := strings.CutPrefix(password, "file:"); ok {
		data, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("reading password file %s: %w", file, err)
		}

		return strings.TrimSpace(string(data)), nil
	}

	return password, nil
}
