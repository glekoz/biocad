package helper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

func Move(from, to, filename string) error {
	sourcePath := filepath.Join(from, filename)
	destPath := filepath.Join(to, filename)
	if err := os.Rename(sourcePath, destPath); err != nil {
		return fmt.Errorf("Перемещение файла: %w", err)
	}
	return nil
}

func Tee[T any](ctx context.Context, val T, chans ...chan T) {
	switch len(chans) {
	case 0:
		return
	case 1:
		select {
		case <-ctx.Done():
		case chans[0] <- val:
		}
	case 2:
		for range 2 {
			select {
			case <-ctx.Done():
				return
			case chans[0] <- val:
				chans[0] = nil
			case chans[1] <- val:
				chans[1] = nil
			}
		}
	default:
		var wg sync.WaitGroup
		wg.Add(len(chans))

		for _, ch := range chans {
			go func() {
				defer wg.Done()
				select {
				case <-ctx.Done():
				case ch <- val:
				}
			}()
		}
		wg.Wait()
	}
}
