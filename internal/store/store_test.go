package store_test

import (
	"context"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestInsertEventThenExists(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if exists {
		t.Fatal("expected event to be absent before insert")
	}

	if _, err := s.InsertEvent(ctx, evt); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	exists, err = s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected event to exist after insert")
	}
}

func TestIncrementAccountStatsAccumulates(t *testing.T) {
	s := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	if err := s.IncrementAccountStats(ctx, accountID, 30); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}
	if err := s.IncrementAccountStats(ctx, accountID, 12); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("got %+v, want CallCount=2 TotalDurationSec=42", got)
	}
}

func TestUpsertCallThenMarkRecordingProcessed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", Payload: []byte(`{}`),
	}
	if err := s.UpsertCall(ctx, evt); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	var processed bool
	row := s.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true")
	}
}

func TestInsertEventDuplicateIsIgnored(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID:     eventID,
		CallID:      callID,
		AccountID:   accountID,
		Status:      "completed",
		DurationSec: 10,
		Payload:     []byte(`{}`),
	}

	inserted, err := s.InsertEvent(ctx, evt)
	if err != nil {
		t.Fatalf("first InsertEvent: %v", err)
	}
	if !inserted {
		t.Fatal("expected first insert to succeed")
	}

	inserted, err = s.InsertEvent(ctx, evt)
	if err != nil {
		t.Fatalf("duplicate InsertEvent: %v", err)
	}
	if inserted {
		t.Fatal("expected duplicate insert to be ignored")
	}

	var count int
	err = s.Pool().QueryRow(
		ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`,
		eventID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}

	if count != 1 {
		t.Fatalf("got %d events, want 1", count)
	}
}
