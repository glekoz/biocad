package service

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/glekoz/biocad/worker/internal/models"
)

type StringLineBatch struct {
	Records [][]string
	Err     error
}

type LineBatch struct {
	Records []models.TSVRecord
	Err     error
}

// func (s *Service) streamTSV(ctx context.Context, filename string) (<-chan LineBatch, <-chan LineBatch, error) {
func (s *Service) streamTSV(ctx context.Context, filename string) (<-chan StringLineBatch, <-chan StringLineBatch, error) {
	file, err := os.Open(filepath.Join(s.SourcePath, filename))
	if err != nil {
		return nil, nil, err
	}
	// out1, out2 := make(chan LineBatch), make(chan LineBatch)
	out1 := make(chan StringLineBatch, 1)
	out2 := make(chan StringLineBatch, 1)
	go func() {
		defer func() {
			r := recover()
			if r != nil {
				s.logger.Error("Panic recovered: %s", r)
			}
		}()
		defer func() {
			file.Close()
			close(out1)
			close(out2)
		}()

		reader := csv.NewReader(file)
		reader.Comma = '\t'      // TSV separator
		reader.LazyQuotes = true // Allow inconsistent quoting

		// batch := make([]models.TSVRecord, 0, s.BatchSize)
		batch := make([][]string, 0, s.BatchSize)
		hs, err := reader.Read() // пропускаем заголовки
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			// ошибка или логируется, или передается дальше
			// s.logger.Error("Чтение первой строки", "err", err.Error())
			err = fmt.Errorf("Чтение первой строки: %w", err)
			for range 2 {
				select {
				case <-ctx.Done():
					return
				// case out1 <- LineBatch{Err: err}:
				case out1 <- StringLineBatch{Err: err}:
					out1 = nil
				// case out2 <- LineBatch{Err: err}:
				case out2 <- StringLineBatch{Err: err}:
					out2 = nil
				}
			}
			return
		}
		ok := s.parser.validateHeaders(hs)
		if !ok {
			err = errors.New("Заголовки не соответствуют ожидаемым")
			for range 2 {
				select {
				case <-ctx.Done():
					return
				// case out1 <- LineBatch{Err: err}:
				case out1 <- StringLineBatch{Err: err}:
					out1 = nil
				// case out2 <- LineBatch{Err: err}:
				case out2 <- StringLineBatch{Err: err}:
					out2 = nil
				}
			}
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
						out11, out22 := out1, out2
						for i := 0; i < 2; i++ {
							select {
							case <-ctx.Done():
								return
							// case out1 <- LineBatch{Records: batch}:
							case out11 <- StringLineBatch{Records: batch}:
								out11 = nil
							// case out2 <- LineBatch{Records: batch}:
							case out22 <- StringLineBatch{Records: batch}:
								out22 = nil
							}
						}
					}
					break
				}
				s.logger.Error("чтение TSV", "err", err.Error())
				out11, out22 := out1, out2
				for i := 0; i < 2; i++ {
					select {
					case <-ctx.Done():
						return
					// case out1 <- LineBatch{Err: err}:
					case out11 <- StringLineBatch{Err: err}:
						out11 = nil
					// case out2 <- LineBatch{Err: err}:
					case out22 <- StringLineBatch{Err: err}:
						out22 = nil
					}
				}
				return
			}

			// парсинг строки в модель, например, TSVRecord
			// record, ok := s.parser.newRecord(line)
			record, ok := s.parser.newStringRecord(line)
			if !ok {
				s.logger.Warn("Строка не соответствует формату", "errs", record)
				continue
			}

			batch = append(batch, record)
			if len(batch) >= s.BatchSize {
				out11, out22 := out1, out2
				for i := 0; i < 2; i++ {
					select {
					case <-ctx.Done():
						return
					// case out1 <- LineBatch{Records: batch}:
					case out11 <- StringLineBatch{Records: batch}:
						out11 = nil
					// case out2 <- LineBatch{Records: batch}:
					case out22 <- StringLineBatch{Records: batch}:
						out22 = nil
					}
				}
				// очистка батча для следующей порции данных
				// если обнулить существующий, то он перезапишет данные, которые уже были отправлены в канал
				// batch = make([]models.TSVRecord, 0, s.BatchSize)
				batch = make([][]string, 0, s.BatchSize)
			}
		}
	}()

	return out1, out2, nil
}
