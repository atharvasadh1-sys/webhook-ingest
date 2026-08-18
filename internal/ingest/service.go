// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

const (
	recordingWork = 50 * time.Millisecond

	recordingQueueKey      = "webhook:recordings"
	recordingProcessingKey = "webhook:recordings:processing"
)

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	workerCtx, cancel := context.WithCancel(context.Background())

	svc := &Service{
		store:  s,
		cache:  c,
		rdb:    rdb,
		log:    log,
		cancel: cancel,
	}

	// Recover jobs that were in-flight when the previous process stopped.
	svc.recoverRecordingJobs(workerCtx)

	svc.wg.Add(1)
	go svc.recordingWorker(workerCtx)

	return svc
}

// Close gracefully stops the recording worker.
func (s *Service) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// enqueueRecording adds a durable recording job to Redis.
func (s *Service) enqueueRecording(ctx context.Context, rec store.Event) error {
	job, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	return s.rdb.RPush(ctx, recordingQueueKey, job).Err()
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and queues recording processing.
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

	if rec.RecordingURL != "" {
		if err := s.enqueueRecording(ctx, rec); err != nil {
			return err
		}
	}

	return nil
}

// recoverRecordingJobs moves jobs left in the processing list back to the queue.
func (s *Service) recoverRecordingJobs(ctx context.Context) {
	for {
		job, err := s.rdb.RPopLPush(
			ctx,
			recordingProcessingKey,
			recordingQueueKey,
		).Result()

		if errors.Is(err, redis.Nil) {
			return
		}

		if err != nil {
			s.log.Error("failed to recover recording jobs", "error", err)
			return
		}

		s.log.Info("recovered recording job", "size", len(job))
	}
}

// recordingWorker processes durable recording jobs.
func (s *Service) recordingWorker(ctx context.Context) {
	defer s.wg.Done()

	for {
		job, err := s.rdb.BRPopLPush(
			ctx,
			recordingQueueKey,
			recordingProcessingKey,
			1*time.Second,
		).Result()

		if errors.Is(err, redis.Nil) {
			continue
		}

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			s.log.Error("recording worker failed", "error", err)
			continue
		}

		var rec store.Event
		if err := json.Unmarshal([]byte(job), &rec); err != nil {
			s.log.Error("invalid recording job", "error", err)

			_ = s.rdb.LRem(
				context.Background(),
				recordingProcessingKey,
				1,
				job,
			).Err()

			continue
		}

		if err := s.processRecording(ctx, rec); err != nil {
			if ctx.Err() != nil {
				return
			}

			s.log.Error(
				"recording processing failed",
				"call_id", rec.CallID,
				"error", err,
			)

			continue
		}

		if err := s.rdb.LRem(
			context.Background(),
			recordingProcessingKey,
			1,
			job,
		).Err(); err != nil {
			s.log.Error(
				"failed to acknowledge recording job",
				"call_id", rec.CallID,
				"error", err,
			)
		}
	}
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	timer := time.NewTimer(recordingWork)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
