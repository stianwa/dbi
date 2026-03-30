package dbi

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Query holds configuration for executing queries.
type Query struct {
	ctx       context.Context
	cancel    context.CancelFunc
	txOptions *sql.TxOptions
	setLocal  *SetLocal
	driver    string
	db        *sql.DB
}

// Option configures Query.
type Option func(q *Query)

// WithSetLocal sets session-local parameters to be applied before
// executing queries.
func WithSetLocal(setLocal *SetLocal) Option {
	return func(q *Query) {
		q.setLocal = setLocal
	}
}

// WithContext sets the context used for query execution.
func WithContext(ctx context.Context) Option {
	return func(q *Query) {
		q.ctx = ctx
	}
}

// WithTimeout sets a timeout on the query context.
//
// If a context is already set, it is used as the parent context.
func WithTimeout(duration time.Duration) Option {
	return func(q *Query) {
		cause := fmt.Errorf("query timed out after %s", duration)

		parent := q.ctx
		if parent == nil {
			parent = context.Background()
		}

		q.ctx, q.cancel = context.WithTimeoutCause(parent, duration, cause)
	}
}

// WithCancelFunc sets a cancel function that is called when query
// execution finishes.
func WithCancelFunc(cancel context.CancelFunc) Option {
	return func(q *Query) {
		q.cancel = cancel
	}
}

// WithTxOptions sets transaction options for query execution.
func WithTxOptions(txOpts *sql.TxOptions) Option {
	return func(q *Query) {
		q.txOptions = txOpts
	}
}

// NewQuery constructs Query from the provided options
// or defaults.
//
// If no context is provided, a background context or a timeout
// context is used, depending on Config defaults. Transaction options
// are also initialized from Config if not explicitly set.
func (c *Config) NewQuery(opts ...Option) *Query {
	q := &Query{}

	for _, opt := range opts {
		opt(q)
	}

	if q.ctx == nil {
		if c.DefaultQueryTimeout == 0 {
			q.ctx = context.Background()
		} else {
			q.ctx, q.cancel = context.WithTimeout(context.Background(), c.DefaultQueryTimeout)
		}
	}

	if q.txOptions == nil {
		txOpts := &sql.TxOptions{
			Isolation: c.DefaultQueryIsolation,
			ReadOnly:  c.DefaultQueryReadOnly,
		}
		if txOpts.ReadOnly || txOpts.Isolation != 0 {
			q.txOptions = txOpts
		}
	}

	q.db = c.db
	q.driver = c.Driver

	return q
}
