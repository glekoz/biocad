package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/glekoz/biocad/worker/internal/repository"
)

// ─── mocks ─────────────────────────────────────────────────────────────────

type mockTx struct {
	mu          sync.Mutex
	insertErr   error
	commitErr   error
	rollbackErr error
	batches     [][][]string
	committed   bool
	rolledBack  bool
}

func (m *mockTx) InsertRecordsBatch(_ context.Context, records [][]string) error {
	if m.insertErr != nil {
		return m.insertErr
	}
	m.mu.Lock()
	m.batches = append(m.batches, records)
	m.mu.Unlock()
	return nil
}

func (m *mockTx) Commit(_ context.Context) error {
	m.mu.Lock()
	m.committed = true
	m.mu.Unlock()
	return m.commitErr
}

func (m *mockTx) Rollback(_ context.Context) error {
	m.mu.Lock()
	m.rolledBack = true
	m.mu.Unlock()
	return m.rollbackErr
}

type mockRepo struct {
	tx       *mockTx
	beginErr error
}

func (m *mockRepo) BeginTx(_ context.Context) (repository.Tx, error) {
	if m.beginErr != nil {
		return nil, m.beginErr
	}
	return m.tx, nil
}

func (m *mockRepo) SaveErroredFile(_ context.Context, _ string, _ string) error {
	return nil
}

// drainErrChan читает ошибку и ождиает закрытия канала
// (закрытие происходит после defer, так что Commit/Rollback выполнены).
func drainErrChan(ch <-chan error) error {
	err, ok := <-ch
	if !ok {
		return nil
	}
	for range ch {
	}
	return err
}

func newSaveDBService(repo RepoAPI) *Service {
	return &Service{
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		repo:      repo,
		BatchSize: 10,
	}
}

func makeBatchChan(batches ...StringLineBatch) chan StringLineBatch {
	ch := make(chan StringLineBatch, len(batches))
	for _, b := range batches {
		ch <- b
	}
	close(ch)
	return ch
}

func sampleBatch() StringLineBatch {
	return StringLineBatch{
		Records: [][]string{
			{"1", "topic", "inv1", "guid-abc", "msg1", "txt", "ctx", "cls", "2", "area", "addr", "blk", "tp", "0", "1"},
		},
	}
}

// ─── tests ─────────────────────────────────────────────────────────────────

func TestSaveToDB_HappyPath(t *testing.T) {
	tx := &mockTx{}
	svc := newSaveDBService(&mockRepo{tx: tx})

	in := makeBatchChan(sampleBatch(), sampleBatch())
	err := drainErrChan(svc.saveToDB(context.Background(), in))

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !tx.committed {
		t.Error("expected Commit to be called")
	}
	if got := len(tx.batches); got != 2 {
		t.Errorf("expected 2 batches inserted, got %d", got)
	}
}

func TestSaveToDB_BeginTxError(t *testing.T) {
	beginErr := errors.New("connection refused")
	svc := newSaveDBService(&mockRepo{beginErr: beginErr})

	in := makeBatchChan()
	err := drainErrChan(svc.saveToDB(context.Background(), in))

	if !errors.Is(err, beginErr) {
		t.Fatalf("expected beginErr, got %v", err)
	}
}

func TestSaveToDB_InsertError(t *testing.T) {
	insertErr := errors.New("insert failed")
	tx := &mockTx{insertErr: insertErr}
	svc := newSaveDBService(&mockRepo{tx: tx})

	in := makeBatchChan(sampleBatch())
	err := drainErrChan(svc.saveToDB(context.Background(), in))

	if !errors.Is(err, insertErr) {
		t.Fatalf("expected insertErr, got %v", err)
	}
	if tx.committed {
		t.Error("expected Commit NOT to be called after insert error")
	}
	if !tx.rolledBack {
		t.Error("expected Rollback to be called after insert error")
	}
}

func TestSaveToDB_BatchCarriesError(t *testing.T) {
	batchErr := errors.New("parse error in stream")
	tx := &mockTx{}
	svc := newSaveDBService(&mockRepo{tx: tx})

	in := makeBatchChan(StringLineBatch{Err: batchErr})
	err := drainErrChan(svc.saveToDB(context.Background(), in))

	if !errors.Is(err, batchErr) {
		t.Fatalf("expected batchErr, got %v", err)
	}
	if tx.committed {
		t.Error("expected Commit NOT to be called after batch error")
	}
	if !tx.rolledBack {
		t.Error("expected Rollback to be called after batch error")
	}
}

func TestSaveToDB_EmptyFile(t *testing.T) {
	tx := &mockTx{}
	svc := newSaveDBService(&mockRepo{tx: tx})

	in := makeBatchChan() // no batches
	err := drainErrChan(svc.saveToDB(context.Background(), in))

	if err != nil {
		t.Fatalf("expected nil for empty input, got %v", err)
	}
	if !tx.committed {
		t.Error("expected Commit even for empty file")
	}
}

func TestSaveToDB_ContextCancelled(t *testing.T) {
	tx := &mockTx{}
	svc := newSaveDBService(&mockRepo{tx: tx})

	ctx, cancel := context.WithCancel(context.Background())

	in := make(chan StringLineBatch)

	result := make(chan error, 1)
	go func() {
		result <- drainErrChan(svc.saveToDB(ctx, in))
	}()

	// симуляция отмены контекста
	cancel()
	close(in)

	select {
	case err := <-result:
		_ = err
		if !tx.committed && !tx.rolledBack {
			t.Error("expected either Commit or Rollback to be called")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("saveToDB did not complete after context cancellation")
	}
}
