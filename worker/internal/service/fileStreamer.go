package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (s *Service) streamFiles(ctx context.Context) <-chan string {
	filesChan := make(chan string, s.MaxWorkers)

	go func() {
		defer func() {
			r := recover()
			if r != nil {
				s.logger.Error("Panic recovered: %s", r)
			}
			close(filesChan)
		}()

		interval := time.Duration(s.PollIntervalMs) * time.Millisecond
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// инициализирую переменные, так как мне нужно далее изменять одни и те же files
		files, err := s.scanDir()
		if err != nil {
			s.logger.Error("Чтение директории", "error", err.Error())
			return
		}
		for {
			s.logger.Info("очередная итерация сканирования", "files", files)
			if len(files) == 0 {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					files, err = s.scanDir() // заполнение переменной
					if err != nil {
						s.logger.Error("Чтение директории", "error", err.Error())
						return
					}
				}
			} else {
				// f := s.getFile(&files)
				var f string
				s.fipMutex.RLock()
				for i := 0; i < len(files); i++ {
					if !s.filesInProgress[files[i]] {
						f = files[i]
						files = files[i+1:]
						break
					}
				}
				s.fipMutex.RUnlock()
				if f == "" {
					files = []string{}
					continue
				}
				// if err != nil {
				// 	s.logger.Error("Получение файла", "error", err.Error())
				// 	continue
				// }
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					files, err = s.scanDir() // заполнение переменной
					if err != nil {
						s.logger.Error("Чтение директории", "error", err.Error())
						return
						// намеренно не использую f ещё раз
						// на случай, если этого файла уже нет
					}
				case filesChan <- f:
					fmt.Println("отдан", f)
				}
			}
		}
	}()
	return filesChan
}

func (s *Service) scanDir() ([]string, error) {
	entries, err := os.ReadDir(s.SourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	// ёмкость равна длине массива с именами файлов, так как предполагается,
	// что в этой директории в основном содержатся .tsv файлы
	filesToProcess := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filename := entry.Name()
		if !strings.HasSuffix(strings.ToLower(filename), ".tsv") {
			continue
		}
		filesToProcess = append(filesToProcess, filename)
	}
	return filesToProcess, nil
}

// func (s *Service) getFile(files *[]string) string {
// 	file := (*files)[0]
// 	*files = (*files)[1:]

// 	return file
// }

func move(from, to, filename string) error {
	sourcePath := filepath.Join(from, filename)
	destPath := filepath.Join(to, filename)
	if err := os.Rename(sourcePath, destPath); err != nil {
		return fmt.Errorf("Перемещение файла: %w", err)
	}
	return nil
}
