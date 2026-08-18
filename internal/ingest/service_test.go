package ingest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()

	resp, err := http.Post(
		url,
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	t.Cleanup(func() {
		_ = resp.Body.Close()
	})

	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(
		ctx,
		`SELECT account_id FROM calls WHERE call_id = $1`,
		callID,
	)

	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}

	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(
		ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`,
		eventID,
	)

	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}

	var callCount int
	var totalDuration int

	row = st.Pool().QueryRow(
		ctx,
		`SELECT call_count, total_duration_sec
		 FROM account_stats
		 WHERE account_id = $1`,
		accountID,
	)

	if err := row.Scan(&callCount, &totalDuration); err != nil {
		t.Fatalf("scan account stats: %v", err)
	}

	if callCount != 1 {
		t.Fatalf("call_count = %d, want 1", callCount)
	}

	if totalDuration != 143 {
		t.Fatalf("total_duration_sec = %d, want 143", totalDuration)
	}
}

func TestConcurrentDuplicateDeliveryIsIdempotent(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const deliveries = 10

	var wg sync.WaitGroup
	responses := make(chan int, deliveries)

	for i := 0; i < deliveries; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			resp, err := http.Post(
				srv.URL+"/webhooks/calls",
				"application/json",
				strings.NewReader(body),
			)
			if err != nil {
				t.Errorf("post: %v", err)
				return
			}
			defer resp.Body.Close()

			responses <- resp.StatusCode
		}()
	}

	wg.Wait()
	close(responses)

	for status := range responses {
		if status != http.StatusOK {
			t.Errorf("got status %d, want 200", status)
		}
	}

	var n int
	row := st.Pool().QueryRow(
		ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`,
		eventID,
	)

	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if n != 1 {
		t.Fatalf("stored %d copies of %s, want exactly 1", n, eventID)
	}

	var callCount int
	var totalDuration int

	row = st.Pool().QueryRow(
		ctx,
		`SELECT call_count, total_duration_sec
		 FROM account_stats
		 WHERE account_id = $1`,
		accountID,
	)

	if err := row.Scan(&callCount, &totalDuration); err != nil {
		t.Fatalf("scan account stats: %v", err)
	}

	if callCount != 1 {
		t.Fatalf("call_count = %d, want 1", callCount)
	}

	if totalDuration != 143 {
		t.Fatalf("total_duration_sec = %d, want 143", totalDuration)
	}
}

func TestRecordingIsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	deadline := time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		var processed bool

		row := st.Pool().QueryRow(
			ctx,
			`SELECT recording_processed
			 FROM calls
			 WHERE call_id = $1`,
			callID,
		)

		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}

		if processed {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("recording should be marked as processed")
}
func TestRecordingJobIsRecoveredFromProcessingList(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	rec := store.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  143,
		RecordingURL: "https://recordings.example.com/test.wav",
		Payload:      []byte(`{}`),
	}

	if err := st.UpsertCall(ctx, rec); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}

	job, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}

	cfg := config.Load()
	rdb, err := redisclient.New(ctx, cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to redis: %v", err)
	}
	defer rdb.Close()

	const (
		processingKey = "webhook:recordings:processing"
		queueKey      = "webhook:recordings"
	)

	if err := rdb.Del(ctx, processingKey, queueKey).Err(); err != nil {
		t.Fatalf("clean recording queues: %v", err)
	}

	t.Cleanup(func() {
		_ = rdb.Del(
			context.Background(),
			processingKey,
			queueKey,
		).Err()
	})

	// Simulate a job left in the processing list by a worker
	// that stopped during deployment.
	if err := rdb.RPush(ctx, processingKey, job).Err(); err != nil {
		t.Fatalf("seed processing job: %v", err)
	}

	// New service startup should recover the job.
	svc := ingest.New(st, stats.NewCache(), rdb, slog.Default())
	defer svc.Close()

	deadline := time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		var processed bool

		row := st.Pool().QueryRow(
			ctx,
			`SELECT recording_processed
			 FROM calls
			 WHERE call_id = $1`,
			callID,
		)

		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}

		if processed {
			remaining, err := rdb.LLen(ctx, processingKey).Result()
			if err != nil {
				t.Fatalf("check processing list: %v", err)
			}

			if remaining != 0 {
				t.Fatalf("processing list still contains %d jobs", remaining)
			}

			queued, err := rdb.LLen(ctx, queueKey).Result()
			if err != nil {
				t.Fatalf("check recording queue: %v", err)
			}

			if queued != 0 {
				t.Fatalf("recording queue still contains %d jobs", queued)
			}

			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("recovered recording job was not processed")
}
