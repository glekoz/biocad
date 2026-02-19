package helper

import (
	"fmt"
	"os"
	"path/filepath"
)

func Move(from, to, filename string) error {
	sourcePath := filepath.Join(from, filename)
	destPath := filepath.Join(to, filename)
	if err := os.Rename(sourcePath, destPath); err != nil {
		return fmt.Errorf("Перемещение файла: %w", err)
	}
	return nil
}
