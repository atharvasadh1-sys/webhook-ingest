// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	svc := &Service{
		store: s,
		cache: c,
		rdb:   rdb,
		log:   log,
	}

	go svc.recordingWorker(context.Background())

	return svc
}
func (s *Service) enqueueRecording(ctx context.Context, rec store.Event) error {
	job, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	return s.rdb.RPush(ctx, "webhook:recordings", job).Err()
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {

	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}
	inserted, err := s.store.InsertEvent(ctx, rec)
	if err != nil {
		return err
	}

	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	if err := s.store.UpsertCall(ctx, rec); err != nil {
		return err
	}
	if err := s.store.IncrementAccountStats(ctx, rec.AccountID, rec.DurationSec); err != nil {
		return err
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		if err := s.enqueueRecording(ctx, rec); err != nil {
			return err
		}
	}

	return nil
}
func (s *Service) recordingWorker(ctx context.Context) {
	for {
		result, err := s.rdb.BLPop(
			ctx,
			0,
			"webhook:recordings",
		).Result()

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			s.log.Error("recording worker failed", "error", err)
			continue
		}

		if len(result) != 2 {
			continue
		}

		var rec store.Event
		if err := json.Unmarshal([]byte(result[1]), &rec); err != nil {
			s.log.Error("invalid recording job", "error", err)
			continue
		}

		if err := s.processRecording(ctx, rec); err != nil {
			s.log.Error(
				"recording processing failed",
				"call_id", rec.CallID,
				"error", err,
			)
		}
	}
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
