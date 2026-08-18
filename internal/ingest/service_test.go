package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

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
