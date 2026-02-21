package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/glekoz/biocad/worker/internal/service/pdf"
)

// ─── helpers ───────────────────────────────────────────────────────────────

func newCreatePDFsService(t *testing.T) (*Service, string) {
	t.Helper()
	pdfDir := t.TempDir()
	headers := []string{
		"n", "mqtt", "invid", "unit_guid", "msg_id",
		"text", "context", "class", "level", "area",
		"addr", "block", "type", "bit", "invert_bit",
	}
	pdfCfg, err := pdf.GetConfig(headers)
	if err != nil {
		t.Fatalf("pdf.GetConfig: %v", err)
	}
	svc := &Service{
		PDFPath:         pdfDir,
		PDFConfig:       pdfCfg,
		BatchSize:       100,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		guidsInProgress: make(map[string]*MutexCounter),
		gipMutex:        &sync.Mutex{},
	}
	return svc, pdfDir
}

func makeRecord(guid string) []string {
	return []string{
		"1", "topic", "inv1", guid, "msg1",
		"hello", "ctx", "classA", "3", "area",
		"addr", "block", "tp", "0", "1",
	}
}

func feedChan(batches ...StringLineBatch) chan StringLineBatch {
	ch := make(chan StringLineBatch, len(batches))
	for _, b := range batches {
		ch <- b
	}
	close(ch)
	return ch
}

func drainErr(ch <-chan error) error {
	for e := range ch {
		if e != nil {
			return e
		}
	}
	return nil
}

func pdfFilesInDir(t *testing.T, pdfDir, guid string) int {
	t.Helper()
	dir := filepath.Join(pdfDir, guid)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".pdf" {
			count++
		}
	}
	return count
}

// ─── tests ─────────────────────────────────────────────────────────────────

func TestCreatePDFs_HappyPath_SingleGUID(t *testing.T) {
	svc, pdfDir := newCreatePDFsService(t)

	in := feedChan(StringLineBatch{
		Records: [][]string{makeRecord("guid-aaa"), makeRecord("guid-aaa")},
	})

	err := drainErr(svc.createPDFs(context.Background(), in))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n := pdfFilesInDir(t, pdfDir, "guid-aaa"); n != 1 {
		t.Errorf("expected 1 PDF for guid-aaa, got %d", n)
	}
}

func TestCreatePDFs_HappyPath_MultipleGUIDs(t *testing.T) {
	svc, pdfDir := newCreatePDFsService(t)

	in := feedChan(StringLineBatch{
		Records: [][]string{
			makeRecord("guid-aaa"),
			makeRecord("guid-bbb"),
			makeRecord("guid-aaa"),
		},
	})

	err := drainErr(svc.createPDFs(context.Background(), in))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n := pdfFilesInDir(t, pdfDir, "guid-aaa"); n != 1 {
		t.Errorf("expected 1 PDF for guid-aaa, got %d", n)
	}
	if n := pdfFilesInDir(t, pdfDir, "guid-bbb"); n != 1 {
		t.Errorf("expected 1 PDF for guid-bbb, got %d", n)
	}
}

func TestCreatePDFs_GUIDSpreadAcrossMultipleBatches(t *testing.T) {
	svc, pdfDir := newCreatePDFsService(t)

	in := feedChan(
		StringLineBatch{Records: [][]string{makeRecord("guid-ccc")}},
		StringLineBatch{Records: [][]string{makeRecord("guid-ccc")}},
	)

	err := drainErr(svc.createPDFs(context.Background(), in))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if n := pdfFilesInDir(t, pdfDir, "guid-ccc"); n != 1 {
		t.Errorf("expected 1 consolidated PDF for guid-ccc, got %d", n)
	}
}

func TestCreatePDFs_EmptyInput(t *testing.T) {
	svc, pdfDir := newCreatePDFsService(t)

	in := feedChan()

	err := drainErr(svc.createPDFs(context.Background(), in))
	if err != nil {
		t.Fatalf("expected nil error for empty input, got %v", err)
	}

	entries, _ := os.ReadDir(pdfDir)
	if len(entries) != 0 {
		t.Errorf("expected no files/dirs in pdfDir for empty input, got %d", len(entries))
	}
}

func TestCreatePDFs_BatchCarriesError(t *testing.T) {
	svc, pdfDir := newCreatePDFsService(t)
	streamErr := errors.New("stream parse error")

	in := feedChan(StringLineBatch{Err: streamErr})

	err := drainErr(svc.createPDFs(context.Background(), in))
	if !errors.Is(err, streamErr) {
		t.Fatalf("expected streamErr to propagate, got: %v", err)
	}

	entries, _ := os.ReadDir(pdfDir)
	if len(entries) != 0 {
		t.Errorf("expected no PDF files after batch error, got %d entries", len(entries))
	}
}

func TestCreatePDFs_CleanupOnPDFSaveError(t *testing.T) {
	svc, pdfDir := newCreatePDFsService(t)

	// Make the guid directory read-only so document.Save fails.
	guidDir := filepath.Join(pdfDir, "guid-ro")
	if err := os.MkdirAll(guidDir, 0555); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Cleanup(func() { os.Chmod(guidDir, 0755) })

	in := feedChan(StringLineBatch{
		Records: [][]string{makeRecord("guid-ro")},
	})

	err := drainErr(svc.createPDFs(context.Background(), in))
	if err == nil {
		t.Skip("OS may allow writes despite 0555; skipping cleanup assertion")
	}

	if n := pdfFilesInDir(t, pdfDir, "guid-ro"); n != 0 {
		t.Errorf("expected 0 PDFs after save error cleanup, got %d", n)
	}
}

func TestCreatePDFs_ContextCancelledWhileReadingBatches(t *testing.T) {
	svc, _ := newCreatePDFsService(t)

	ctx, cancel := context.WithCancel(context.Background())

	in := make(chan StringLineBatch)
	go func() {
		defer close(in)
		in <- StringLineBatch{Records: [][]string{makeRecord("guid-x")}}
		cancel()
	}()

	done := make(chan error, 1)
	go func() { done <- drainErr(svc.createPDFs(ctx, in)) }()

	select {
	case <-done:
		// completed without hanging
	case <-time.After(3 * time.Second):
		t.Fatal("createPDFs did not stop after context cancellation")
	}
}

func TestCreatePDFs_ContextAlreadyCancelled(t *testing.T) {
	svc, _ := newCreatePDFsService(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := make(chan StringLineBatch)
	close(in) // closed immediately to avoid blocking the range

	done := make(chan error, 1)
	go func() { done <- drainErr(svc.createPDFs(ctx, in)) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("createPDFs hung with already-cancelled context")
	}
}

func TestCreatePDFs_GUIDsInProgressCleanedUp(t *testing.T) {
	svc, _ := newCreatePDFsService(t)

	in := feedChan(StringLineBatch{
		Records: [][]string{makeRecord("guid-cleanup"), makeRecord("guid-cleanup")},
	})

	err := drainErr(svc.createPDFs(context.Background(), in))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	svc.gipMutex.Lock()
	_, exists := svc.guidsInProgress["guid-cleanup"]
	svc.gipMutex.Unlock()

	if exists {
		t.Error("expected guid-cleanup to be removed from guidsInProgress after completion")
	}
}
