// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// Close releases all pooled connections.
func (s *Store) Close() {
	s.pool.Close()
}

// EventExists reports whether an event with this ID has already been stored.
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	var one int

	err := s.pool.QueryRow(
		ctx,
		`SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`,
		eventID,
	).Scan(&one)

	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// InsertEvent stores the raw delivery.
// It returns true only when the event was actually inserted.
func (s *Store) InsertEvent(ctx context.Context, e Event) (bool, error) {
	var insertedID string

	err := s.pool.QueryRow(
		ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING event_id`,
		e.EventID,
		e.CallID,
		e.AccountID,
		e.Payload,
	).Scan(&insertedID)

	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// UpsertCall creates or refreshes the call record for this event.
func (s *Store) UpsertCall(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(
		ctx,
		`INSERT INTO calls (
			call_id,
			account_id,
			status,
			duration_sec,
			recording_url,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (call_id) DO UPDATE SET
			status        = EXCLUDED.status,
			duration_sec  = EXCLUDED.duration_sec,
			recording_url = EXCLUDED.recording_url,
			updated_at    = now()`,
		e.CallID,
		e.AccountID,
		e.Status,
		e.DurationSec,
		e.RecordingURL,
	)

	return err
}

// MarkRecordingProcessed flags the call's recording as handled.
func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(
		ctx,
		`UPDATE calls
		 SET recording_processed = TRUE,
		     updated_at = now()
		 WHERE call_id = $1`,
		callID,
	)

	return err
}

// IncrementAccountStats folds one completed call into the durable aggregate.
func (s *Store) IncrementAccountStats(
	ctx context.Context,
	accountID string,
	durationSec int,
) error {
	_, err := s.pool.Exec(
		ctx,
		`INSERT INTO account_stats (
			account_id,
			call_count,
			total_duration_sec
		)
		VALUES ($1, 1, $2)
		ON CONFLICT (account_id) DO UPDATE SET
			call_count = account_stats.call_count + 1,
			total_duration_sec =
				account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		accountID,
		durationSec,
	)

	return err
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(
	ctx context.Context,
	accountID string,
) (Stats, error) {
	var st Stats

	err := s.pool.QueryRow(
		ctx,
		`SELECT call_count, total_duration_sec
		 FROM account_stats
		 WHERE account_id = $1`,
		accountID,
	).Scan(&st.CallCount, &st.TotalDurationSec)

	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}

	if err != nil {
		return Stats{}, err
	}

	return st, nil
}

// ProcessEvent atomically stores the event, updates the call, and increments
// account statistics. It returns true only when the event was newly inserted.
func (s *Store) ProcessEvent(ctx context.Context, e Event) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var insertedID string

	err = tx.QueryRow(
		ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING event_id`,
		e.EventID,
		e.CallID,
		e.AccountID,
		e.Payload,
	).Scan(&insertedID)

	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}

	if err != nil {
		return false, err
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO calls (
			call_id,
			account_id,
			status,
			duration_sec,
			recording_url,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (call_id) DO UPDATE SET
			status        = EXCLUDED.status,
			duration_sec  = EXCLUDED.duration_sec,
			recording_url = EXCLUDED.recording_url,
			updated_at    = now()`,
		e.CallID,
		e.AccountID,
		e.Status,
		e.DurationSec,
		e.RecordingURL,
	)
	if err != nil {
		return false, err
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO account_stats (
			account_id,
			call_count,
			total_duration_sec
		)
		VALUES ($1, 1, $2)
		ON CONFLICT (account_id) DO UPDATE SET
			call_count = account_stats.call_count + 1,
			total_duration_sec =
				account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		e.AccountID,
		e.DurationSec,
	)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}

	return true, nil
}

// AllAccountStats returns the durable account aggregates.
func (s *Store) AllAccountStats(ctx context.Context) (map[string]Stats, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT account_id, call_count, total_duration_sec
		 FROM account_stats`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]Stats)

	for rows.Next() {
		var accountID string
		var st Stats

		if err := rows.Scan(
			&accountID,
			&st.CallCount,
			&st.TotalDurationSec,
		); err != nil {
			return nil, err
		}

		result[accountID] = st
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
