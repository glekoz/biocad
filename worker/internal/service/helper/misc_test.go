package helper_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/glekoz/biocad/worker/internal/service/helper"
)

// ─── Move ──────────────────────────────────────────────────────────────────

func TestMove_HappyPath(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	file := "data.tsv"
	content := []byte("hello")
	if err := os.WriteFile(filepath.Join(src, file), content, 0644); err != nil {
		t.Fatal(err)
	}

	if err := helper.Move(src, dst, file); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(src, file)); !os.IsNotExist(err) {
		t.Error("file should have been removed from source dir")
	}

	got, err := os.ReadFile(filepath.Join(dst, file))
	if err != nil {
		t.Fatalf("file not found in destination: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("file content mismatch: got %q", got)
	}
}

func TestMove_SourceNotExist(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	err := helper.Move(src, dst, "nonexistent.tsv")
	if err == nil {
		t.Fatal("expected error when source file does not exist")
	}
}

func TestMove_DestDirNotExist(t *testing.T) {
	src := t.TempDir()

	file := "data.tsv"
	if err := os.WriteFile(filepath.Join(src, file), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	err := helper.Move(src, "/nonexistent/destination/dir", file)
	if err == nil {
		t.Fatal("expected error when destination dir does not exist")
	}
}

// ─── Tee ───────────────────────────────────────────────────────────────────

func TestTee_ZeroChannels(t *testing.T) {
	ctx := context.Background()
	// не должно паниковать, даже если не передано ни одного канала
	helper.Tee(ctx, "value")
}

func TestTee_OneChannel(t *testing.T) {
	ctx := context.Background()
	ch := make(chan string, 1)

	helper.Tee(ctx, "hello", ch)

	select {
	case got := <-ch:
		if got != "hello" {
			t.Errorf("expected %q, got %q", "hello", got)
		}
	default:
		t.Fatal("expected value on channel")
	}
}

func TestTee_TwoChannels(t *testing.T) {
	ctx := context.Background()
	ch1 := make(chan int, 1)
	ch2 := make(chan int, 1)

	helper.Tee(ctx, 42, ch1, ch2)

	for i, ch := range []chan int{ch1, ch2} {
		select {
		case got := <-ch:
			if got != 42 {
				t.Errorf("ch%d: expected 42, got %d", i+1, got)
			}
		default:
			t.Errorf("ch%d: expected value but channel was empty", i+1)
		}
	}
}

func TestTee_TwoChannels_BothReceive(t *testing.T) {
	ctx := context.Background()
	ch1 := make(chan string, 1)
	ch2 := make(chan string, 1)

	done := make(chan struct{})
	go func() {
		helper.Tee(ctx, "broadcast", ch1, ch2)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Tee did not complete in time")
	}

	if got := <-ch1; got != "broadcast" {
		t.Errorf("ch1: expected %q, got %q", "broadcast", got)
	}
	if got := <-ch2; got != "broadcast" {
		t.Errorf("ch2: expected %q, got %q", "broadcast", got)
	}
}

func TestTee_ContextCancelled_DoesNotHang(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch1 := make(chan string)
	ch2 := make(chan string)

	done := make(chan struct{})
	go func() {
		helper.Tee(ctx, "value", ch1, ch2)
		close(done)
	}()

	select {
	case <-done:
		// Tee не задедлочилась
	case <-time.After(2 * time.Second):
		t.Fatal("Tee hung despite cancelled context")
	}
}
