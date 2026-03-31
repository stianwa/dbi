package dbi

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// QueryOptions holds configuration for executing queries.
type QueryOptions struct {
	ctx       context.Context
	cancel    context.CancelFunc
	txOptions *sql.TxOptions
	setConfig *SetConfig
	driver    string
	db        *sql.DB
}

// Option configures QueryOptions.
type Option func(q *QueryOptions)

// Cancel cancels the query context, if set.
func (q *QueryOptions) Cancel() {
	if q.cancel != nil {
		q.cancel()
	}
}

// WithSetConfig sets session-local parameters to be applied before
// executing queries.
func WithSetConfig(setConfig *SetConfig) Option {
	return func(q *QueryOptions) {
		q.setConfig = setConfig
	}
}

// WithContext sets the context used for query execution.
func WithContext(ctx context.Context) Option {
	return func(q *QueryOptions) {
		q.ctx = ctx
	}
}

// WithTimeout sets a timeout on the query context.
//
// If a context is already set, it is used as the parent context.
func WithTimeout(duration time.Duration) Option {
	return func(q *QueryOptions) {
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
	return func(q *QueryOptions) {
		q.cancel = cancel
	}
}

// WithTxOptions sets transaction options for query execution.
func WithTxOptions(txOpts *sql.TxOptions) Option {
	return func(q *QueryOptions) {
		q.txOptions = txOpts
	}
}

// NewQueryOptions constructs QueryOptions from the provided options
// or defaults.
//
// If no context is provided, a background context or a timeout
// context is used, depending on Config defaults. Transaction options
// are also initialized from Config if not explicitly set.
func (c *Config) NewQueryOptions(opts ...Option) *QueryOptions {
	q := &QueryOptions{}

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
