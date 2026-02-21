package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const tsvHeader = "n\tmqtt\tinvid\tunit_guid\tmsg_id\ttext\tcontext\tclass\tlevel\tarea\taddr\tblock\ttype\tbit\tinvert_bit"

func tsvRow(n string) string {
	return n + "\ttopic\tinv1\tguid-abc\tmsg1\thello\tctx\tclsA\t3\tarea\taddr\tblk\ttp\t0\t1"
}

func writeTSV(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp tsv: %v", err)
	}
	return path
}

func newStreamTestService(dir string, batchSize int) *Service {
	headers := []string{
		"n", "mqtt", "invid", "unit_guid", "msg_id",
		"text", "context", "class", "level", "area",
		"addr", "block", "type", "bit", "invert_bit",
	}
	return &Service{
		SourcePath:      dir,
		BatchSize:       batchSize,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		parser:          NewParser(headers),
		fipMutex:        &sync.RWMutex{},
		filesInProgress: make(map[string]bool),
	}
}

func collectBatches(ch <-chan StringLineBatch) ([][]string, error) {
	var all [][]string
	for b := range ch {
		if b.Err != nil {
			return nil, b.Err
		}
		all = append(all, b.Records...)
	}
	return all, nil
}

// ─── streamTSV ─────────────────────────────────────────────────────────────

func TestStreamTSV_FileNotFound(t *testing.T) {
	dir := t.TempDir()
	svc := newStreamTestService(dir, 100)

	_, _, err := svc.streamTSV(context.Background(), "no_such_file.tsv")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestStreamTSV_InvalidHeaders(t *testing.T) {
	dir := t.TempDir()
	writeTSV(t, dir, "bad.tsv", "wrong\theaders\there\n1\t2\t3\n")
	svc := newStreamTestService(dir, 100)

	ch1, ch2, err := svc.streamTSV(context.Background(), "bad.tsv")
	if err != nil {
		t.Fatalf("unexpected open error: %v", err)
	}

	var wg sync.WaitGroup
	checkInvalidFormat := func(ch <-chan StringLineBatch) {
		defer wg.Done()
		for b := range ch {
			if errors.Is(b.Err, ErrInvalidFileFormat) {
				return
			}
		}
		t.Error("expected ErrInvalidFileFormat on channel")
	}

	wg.Add(2)
	go checkInvalidFormat(ch1)
	go checkInvalidFormat(ch2)
	wg.Wait()
}

func TestStreamTSV_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	writeTSV(t, dir, "empty.tsv", "")
	svc := newStreamTestService(dir, 100)

	ch1, ch2, err := svc.streamTSV(context.Background(), "empty.tsv")
	if err != nil {
		t.Fatalf("unexpected open error: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range ch1 {
		}
	}()
	go func() {
		defer wg.Done()
		for range ch2 {
		}
	}()
	wg.Wait() // просто проверяем, что не зависает и каналы закрываются
}

func TestStreamTSV_ValidFile_AllRecordsReceived(t *testing.T) {
	dir := t.TempDir()
	rows := []string{tsvHeader, tsvRow("1"), tsvRow("2"), tsvRow("3")}
	writeTSV(t, dir, "data.tsv", strings.Join(rows, "\n")+"\n")
	svc := newStreamTestService(dir, 100)

	ch1, ch2, err := svc.streamTSV(context.Background(), "data.tsv")
	if err != nil {
		t.Fatalf("unexpected open error: %v", err)
	}

	var (
		recs1, recs2 [][]string
		err1, err2   error
		wg           sync.WaitGroup
	)
	wg.Add(2)
	go func() { defer wg.Done(); recs1, err1 = collectBatches(ch1) }()
	go func() { defer wg.Done(); recs2, err2 = collectBatches(ch2) }()
	wg.Wait()

	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected errors: ch1=%v ch2=%v", err1, err2)
	}
	if len(recs1) != 3 || len(recs2) != 3 {
		t.Errorf("expected 3 records on each channel, got ch1=%d ch2=%d", len(recs1), len(recs2))
	}
}

func TestStreamTSV_Batching(t *testing.T) {
	dir := t.TempDir()
	// 5 с размером партии 2 -> [2, 2, 1]
	rows := []string{tsvHeader, tsvRow("1"), tsvRow("2"), tsvRow("3"), tsvRow("4"), tsvRow("5")}
	writeTSV(t, dir, "batched.tsv", strings.Join(rows, "\n")+"\n")
	svc := newStreamTestService(dir, 2)

	ch1, ch2, err := svc.streamTSV(context.Background(), "batched.tsv")
	if err != nil {
		t.Fatalf("unexpected open error: %v", err)
	}

	var batches1 []StringLineBatch
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for b := range ch1 {
			batches1 = append(batches1, b)
		}
	}()
	go func() {
		defer wg.Done()
		for range ch2 {
		}
	}()
	wg.Wait()

	totalRecs := 0
	for _, b := range batches1 {
		totalRecs += len(b.Records)
	}
	if totalRecs != 5 {
		t.Errorf("expected 5 total records across batches, got %d", totalRecs)
	}
	if len(batches1) < 2 {
		t.Errorf("expected at least 2 batches with batchSize=2 and 5 records, got %d", len(batches1))
	}
}

func TestStreamTSV_InvalidRowsSkipped(t *testing.T) {
	dir := t.TempDir()
	// во второй строке n не число, должно пропуститься, остальные 2 строки валидные
	badRow := "not-a-number\ttopic\tinv1\tguid-abc\tmsg1\thello\tctx\tclsA\t3\tarea\taddr\tblk\ttp\t0\t1"
	rows := []string{tsvHeader, tsvRow("1"), badRow, tsvRow("3")}
	writeTSV(t, dir, "mixed.tsv", strings.Join(rows, "\n")+"\n")
	svc := newStreamTestService(dir, 100)

	ch1, ch2, err := svc.streamTSV(context.Background(), "mixed.tsv")
	if err != nil {
		t.Fatalf("unexpected open error: %v", err)
	}

	var recs [][]string
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		recs, _ = collectBatches(ch1)
	}()
	go func() {
		defer wg.Done()
		for range ch2 {
		}
	}()
	wg.Wait()

	if len(recs) != 2 {
		t.Errorf("expected 2 valid records (1 bad row skipped), got %d", len(recs))
	}
}

func TestStreamTSV_ContextCancelled(t *testing.T) {
	dir := t.TempDir()
	header := tsvHeader
	var sb strings.Builder
	sb.WriteString(header + "\n")
	for i := 1; i <= 500; i++ {
		sb.WriteString(tsvRow("1") + "\n")
	}
	writeTSV(t, dir, "big.tsv", sb.String())

	svc := newStreamTestService(dir, 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before streaming starts

	ch1, ch2, err := svc.streamTSV(ctx, "big.tsv")
	if err != nil {
		t.Fatalf("unexpected open error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range ch1 {
			}
		}()
		go func() {
			defer wg.Done()
			for range ch2 {
			}
		}()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// каналы не зависли
	case <-time.After(3 * time.Second):
		t.Fatal("streamTSV did not stop after context cancellation")
	}
}

// ─── scanDir ───────────────────────────────────────────────────────────────

func TestScanDir_ReturnsTSVOnly(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "a.tsv"), nil, 0644)
	_ = os.WriteFile(filepath.Join(dir, "b.TSV"), nil, 0644)
	_ = os.WriteFile(filepath.Join(dir, "c.csv"), nil, 0644)
	_ = os.WriteFile(filepath.Join(dir, "d.txt"), nil, 0644)
	svc := newStreamTestService(dir, 10)

	files, err := svc.scanDir()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 .tsv files, got %d: %v", len(files), files)
	}
}

func TestScanDir_SkipsDirs(t *testing.T) {
	dir := t.TempDir()
	_ = os.Mkdir(filepath.Join(dir, "subdir.tsv"), 0755) // dir with .tsv suffix
	_ = os.WriteFile(filepath.Join(dir, "real.tsv"), nil, 0644)
	svc := newStreamTestService(dir, 10)

	files, err := svc.scanDir()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 1 || files[0] != "real.tsv" {
		t.Errorf("expected only real.tsv, got %v", files)
	}
}

func TestScanDir_SkipsFilesInProgress(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "a.tsv"), nil, 0644)
	_ = os.WriteFile(filepath.Join(dir, "b.tsv"), nil, 0644)
	svc := newStreamTestService(dir, 10)
	svc.filesInProgress["a.tsv"] = true

	files, err := svc.scanDir()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 1 || files[0] != "b.tsv" {
		t.Errorf("expected only b.tsv, got %v", files)
	}
}

func TestScanDir_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	svc := newStreamTestService(dir, 10)

	files, err := svc.scanDir()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected empty slice, got %v", files)
	}
}
