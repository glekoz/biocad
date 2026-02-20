package service

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/glekoz/biocad/worker/internal/service/helper"
)

type StringLineBatch struct {
	Records [][]string
	Err     error
}

func (s *Service) streamTSV(ctx context.Context, filename string) (<-chan StringLineBatch, <-chan StringLineBatch, error) {
	file, err := os.Open(filepath.Join(s.SourcePath, filename))
	if err != nil {
		return nil, nil, err
	}
	out1 := make(chan StringLineBatch)
	out2 := make(chan StringLineBatch)
	go func() {
		defer func() {
			r := recover()
			if r != nil {
				s.logger.Error("Panic recovered in streamTSV: %v", r)
			}
		}()
		defer func() {
			err := file.Close()
			if err != nil {
				s.logger.ErrorContext(ctx, "Close file", "error", err)
			}
			close(out1)
			close(out2)
		}()

		reader := csv.NewReader(file)
		reader.Comma = '\t'
		reader.LazyQuotes = true

		batch := make([][]string, 0, s.BatchSize)
		hs, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			helper.Tee(ctx, StringLineBatch{Err: err}, out1, out2)
			return
		}
		ok := s.parser.validateHeaders(hs)
		if !ok {
			helper.Tee(ctx, StringLineBatch{Err: ErrInvalidFileFormat}, out1, out2)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			line, err := reader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					if len(batch) > 0 {
						helper.Tee(ctx, StringLineBatch{Records: batch}, out1, out2)
					}
					break
				}
				helper.Tee(ctx, StringLineBatch{Err: err}, out1, out2)
				return
			}

			// валидация строки и преобразование её в нужную структуру
			record, ok := s.parser.parse(line)
			if !ok {
				s.logger.WarnContext(ctx, "Строка не соответствует формату", "errs", record)
				continue
			}

			batch = append(batch, record)
			if len(batch) == s.BatchSize {
				helper.Tee(ctx, StringLineBatch{Records: batch}, out1, out2)
				// очистка батча для следующей порции данных
				// если обнулить существующий, то он перезапишет данные, которые уже были отправлены в канал
				batch = make([][]string, 0, s.BatchSize)
			}
		}
	}()

	return out1, out2, nil
}

func (s *Service) streamFiles(ctx context.Context) <-chan string {
	filesChan := make(chan string, s.MaxWorkers)

	go func() {
		defer func() {
			r := recover()
			if r != nil {
				s.logger.Error("Panic recovered in streamFiles: %v", r)
			}
			close(filesChan)
		}()

		interval := time.Duration(s.PollIntervalMs) * time.Millisecond
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// инициализирую переменные, так как мне нужно далее изменять одни и те же files
		files, err := s.scanDir()
		if err != nil {
			s.logger.ErrorContext(ctx, "Чтение директории", "error", err.Error())
			return
		}
		s.logger.InfoContext(ctx, "Сканирование", "Количество .tsv файлов", len(files))
		for {
			if len(files) == 0 {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					files, err = s.scanDir()
					if err != nil {
						s.logger.ErrorContext(ctx, "Чтение директории", "error", err.Error())
						return
					}
					s.logger.InfoContext(ctx, "Сканирование", "Количество .tsv файлов", len(files))
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					files, err = s.scanDir()
					if err != nil {
						s.logger.ErrorContext(ctx, "Чтение директории", "error", err.Error())
						return
					}
					s.logger.InfoContext(ctx, "Сканирование", "Количество .tsv файлов", len(files))
					// намеренно не использую f ещё раз
					// на случай, если этого файла уже нет
				case filesChan <- files[0]:
					files = files[1:]
				}
			}
		}
	}()
	return filesChan
}

func (s *Service) scanDir() ([]string, error) {
	entries, err := os.ReadDir(s.SourcePath)
	if err != nil {
		return nil, err
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
		s.fipMutex.RLock()
		if s.filesInProgress[filename] {
			s.fipMutex.RUnlock()
			continue
		}
		s.fipMutex.RUnlock()
		filesToProcess = append(filesToProcess, filename)
	}
	return filesToProcess, nil
}
