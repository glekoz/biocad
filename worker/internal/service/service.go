package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/glekoz/biocad/worker/config"
	"github.com/glekoz/biocad/worker/internal/service/pdf"
	"github.com/rs/xid"
)

type MutexCounter struct {
	mu    *sync.Mutex
	count int
}

type RepoAPI interface {
}

type Service struct {
	SourcePath     string
	CompletedPath  string
	ErrorPath      string // в этой директории также должен быть файл, в котором были бы записаны имена файлов, при перемещении которых возникла ошибка
	PDFPath        string
	PDFConfig      *pdf.Config
	PollIntervalMs int
	MaxWorkers     int
	BatchSize      int

	logger    *slog.Logger
	repo      RepoAPI
	semaphore chan struct{}
	parser    *Parser

	filesInProgress map[string]bool
	fipMutex        *sync.RWMutex

	guidsInProgress map[string]*MutexCounter // для блокировки работы с директориями [guid]mu
	gipMutex        *sync.Mutex
}

func NewService(cfg config.Worker, log *slog.Logger, r RepoAPI) (*Service, error) {
	cfg.FromPath = filepath.Clean(cfg.FromPath)
	if !filepath.IsAbs(cfg.FromPath) {
		return nil, fmt.Errorf("Provided dir path is not absolute: %s", cfg.FromPath)
	}
	fileinfo, err := os.Stat(cfg.FromPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("Provided dir path doesn't exist: %s", cfg.FromPath)
		}
		return nil, err
	}
	if !fileinfo.IsDir() {
		return nil, fmt.Errorf("Provided dir path is not a dir: %s", cfg.FromPath)
	}

	pdfPath := filepath.Join(cfg.FromPath, "pdfs")
	err = os.MkdirAll(pdfPath, 0755)
	if err != nil {
		return nil, err
	}
	completedPath := filepath.Join(cfg.FromPath, "completed")
	err = os.MkdirAll(completedPath, 0755)
	if err != nil {
		return nil, err
	}
	errorPath := filepath.Join(cfg.FromPath, "errors")
	err = os.MkdirAll(errorPath, 0755)
	if err != nil {
		return nil, err
	}

	headers := []string{"n", "mqtt", "invid", "unit_guid", "msg_id", "text", "context", "class", "level", "area", "addr", "block", "type", "bit", "invert_bit"}

	pdfConf, err := pdf.GetConfig(headers)
	if err != nil {
		return nil, fmt.Errorf("Failed to get PDF config: %w", err)
	}

	return &Service{
		SourcePath:     cfg.FromPath,
		CompletedPath:  completedPath,
		ErrorPath:      errorPath,
		PDFPath:        pdfPath,
		PDFConfig:      pdfConf,
		PollIntervalMs: cfg.PollIntervalMs,
		MaxWorkers:     cfg.MaxWorkers,
		BatchSize:      cfg.BatchSize,

		logger:    log,
		repo:      r,
		semaphore: make(chan struct{}, cfg.MaxWorkers),
		parser:    NewParser(headers),

		filesInProgress: make(map[string]bool, cfg.MaxWorkers), // не забыть очистить после обработки
		fipMutex:        &sync.RWMutex{},

		guidsInProgress: make(map[string]*MutexCounter),
		gipMutex:        &sync.Mutex{},
	}, nil
}

// эффективность потоковой обработки снижена из-за того, что
// в бесплатных инструментах для генерации pdf нет возможности писать в файл по частям,
// а нужно генерировать весь документ целиком, что требует хранения всех данных в памяти
func (s *Service) Run(ctx context.Context) {
	s.logger.Info("Service is running", "fromPath", s.SourcePath, "pollInterval", s.PollIntervalMs, "maxWorkers", s.MaxWorkers, "batchSize", s.BatchSize)

	fileStream := s.streamFiles(ctx)

	wg := &sync.WaitGroup{}
	for filename := range fileStream {
		s.semaphore <- struct{}{}

		s.fipMutex.Lock()
		s.filesInProgress[filename] = true
		s.fipMutex.Unlock()

		wg.Add(1)
		go func(f string) {
			// В этой горутине в случае ошибки перемещать файл
			defer func() {
				<-s.semaphore
				wg.Done()
			}()
			// мб добавить настройку в .env для таймаута
			iterCtx, iterCancel := context.WithTimeout(ctx, 5*time.Minute)
			defer iterCancel()

			tsvChan1, tsvChan2, err := s.streamTSV(iterCtx, f)
			if err != nil {
				s.logger.Error("Failed to parse file", "file", f, "error", err)
				return
			}
			errChanPDF := s.createPDFs(iterCtx, tsvChan1, f)
			errChanDB := s.saveToDB(iterCtx, tsvChan2, f)

			// в данном случае поддерживаю консистентное состояние, при котором
			// файл считается обработанным, только если оба канала ошибок вернули nil
			// сначала жду ошибку из канала сохранения в БД, так как это более критичная операция, и если она не удалась, то нет смысла создавать PDF
			var finalErr error
			for range 2 {
				if finalErr != nil {
					iterCancel()
					break
				}
				select {
				case err := <-errChanDB:
					if err != nil {
						s.logger.Error("Failed to save to DB", "file", f, "error", err)
						finalErr = err
					}
					errChanDB = nil
				case err := <-errChanPDF:
					if err != nil {
						s.logger.Error("Failed to save to PDF", "file", f, "error", err)
						finalErr = err
					}
					errChanPDF = nil
				case <-ctx.Done():
					s.logger.Error("Processing timed out", "file", f)
					finalErr = ctx.Err()
				}
			}

			// case err := <-errChanDB:
			// 	if err != nil {
			// 		s.logger.Error("Failed to save to DB", "file", f, "error", err)
			// 		finalErr = err
			// 	}
			// case <-ctx.Done():
			// 	s.logger.Error("Processing timed out", "file", f)
			// 	finalErr = ctx.Err()
			// }
			// if finalErr == nil {
			// 	select {
			// 	case err := <-errChanPDF:
			// 		if err != nil {
			// 			s.logger.Error("Failed to save to PDF", "file", f, "error", err)
			// 			finalErr = err
			// 		}
			// 	case <-ctx.Done():
			// 		s.logger.Error("Processing timed out", "file", f)
			// 		finalErr = ctx.Err()
			// 	}
			// }

			// на этом этапе файл уже обработан
			// если произошла ошибка при перемещении файла,
			// то он остается в мапе и не будет обрабатываться повторно
			if finalErr != nil {
				err = move(s.SourcePath, s.ErrorPath, f) // перемещаю файл в папку с ошибками
				if err != nil {
					s.logger.Error("Failed to move file to errors directory", "file", f, "error", err)
					return
				}
			} else {
				err = move(s.SourcePath, s.CompletedPath, f)
				if err != nil {
					s.logger.Error("Failed to move file to completed directory", "file", f, "error", err)
					return
				}
			}

			s.fipMutex.Lock()
			delete(s.filesInProgress, f)
			s.fipMutex.Unlock()

			s.logger.Info("Successfully processed file", "file", f)
		}(filename)

	}
	wg.Wait()
}

// func (s *Service) createPDFs(ctx context.Context, in <-chan LineBatch) <-chan error {
func (s *Service) createPDFs(ctx context.Context, in <-chan StringLineBatch, filename string) <-chan error {
	out := make(chan error, 1)

	// если батч ерр или отмена контекста, то действия одинаковые
	// если тут произошла ошибка, то крит еррор не нил и просто читаю канал, чтобы
	// не блокировать второй канал
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("Panic recovered in createPDFs: %v", r)
			}
			close(out)
		}()

		var critErr error
		pdfguids := make(map[string]string) // мапа с ключом-guid и xid-файлом, которые добавляются из текущего файла - которую пускаю через ос.ремув
		// recordsByID := make(map[string][]models.TSVRecord)
		recordsByID := make(map[string][][]string)

		defer func() {
			if critErr != nil {
				// fmt.Println(pdfguids)
				for guid, seq := range pdfguids {
					dir := filepath.Join(s.PDFPath, guid)
					if seq != "" {
						p := filepath.Join(dir, seq+".pdf")
						err := os.Remove(p)
						if err != nil {
							s.logger.Error("Failed to remove PDF file during cleanup", "file", p, "error", err)
							return
						}
					}

					s.gipMutex.Lock()
					mc, exists := s.guidsInProgress[guid]
					if exists {
						mc.count++
					} else {
						mc = &MutexCounter{mu: &sync.Mutex{}, count: 1}
						s.guidsInProgress[guid] = mc
					}
					s.gipMutex.Unlock()

					mc.mu.Lock()
					entries, err := os.ReadDir(dir)
					if err != nil {
						s.logger.Error("Failed to read PDF directory during cleanup", "dir", dir, "error", err)
					} else if len(entries) == 0 {
						err = os.Remove(dir)
						if err != nil {
							s.logger.Error("Failed to remove PDF directory during cleanup", "dir", dir, "error", err)
						}
					}
					mc.mu.Unlock()
				}
			}
			s.gipMutex.Lock()
			for guid := range pdfguids {
				mc, exists := s.guidsInProgress[guid]
				if exists {
					mc.count--
					if mc.count == 0 {
						delete(s.guidsInProgress, guid)
					}
				}
			}
			s.gipMutex.Unlock()
		}()

	outermost:
		for batch := range in {
			// если возможно продолжить работу при ошибке,
			// то можно продолжить считывать из канала, но не обрабатывать данные
			// для этого в последних ошибках надо убрать break outermost

			// if critErr != nil {
			// 	continue
			// }
			if batch.Err != nil {
				critErr = batch.Err
				s.logger.Error("Error in createPDFs batch", "error", critErr)
				break outermost
			}
			for i, rec := range batch.Records {
				if i%(s.BatchSize>>1) == 0 {
					if ctx.Err() != nil {
						critErr = ctx.Err()
						s.logger.Error("Context error in createPDFs", "error", critErr)
						break outermost
					}
				}
				if recordsByID[rec[3]] == nil {
					recordsByID[rec[3]] = [][]string{rec}
				} else {
					recordsByID[rec[3]] = append(recordsByID[rec[3]], rec)
				}
			}

			// как будто есть потенциал для распараллеливания
			for guid, recs := range recordsByID {
				if ctx.Err() != nil {
					critErr = ctx.Err()
					s.logger.Error("Context error in createPDFs", "error", critErr)
					break outermost
				}

				seq := xid.New().String()
				dir := filepath.Join(s.PDFPath, guid)
				err := os.MkdirAll(dir, 0755)
				if err != nil {
					critErr = err
					s.logger.Error("Failed to create PDF directory", "guid", guid, "error", err)
					break outermost
				}

				// пустая строка нужна, чтобы появилась запись в мапе
				// чтобы удалить лишнюю директорию в случае ошибки, так как она создается до генерации PDF
				pdfguids[guid] = ""
				pdfPath := filepath.Join(dir, seq+".pdf")

				pdfer := pdf.NewHandler(s.PDFConfig)
				pdfer.AddTitleAndHeader(fmt.Sprintf("Results for unit %s from file %s", guid, filename))
				pdfer.AddDataRows(recs)
				document, err := pdfer.Generate()
				if err != nil {
					critErr = err
					s.logger.Error("Failed to generate PDF document", "guid", guid, "error", err)
					break outermost
				}

				// на случай, если в ДРУГИХ ФАЙЛАХ также есть одинаковый guid
				s.gipMutex.Lock()
				mc, exists := s.guidsInProgress[guid]
				if exists {
					mc.count++
				} else {
					mc = &MutexCounter{mu: &sync.Mutex{}, count: 1}
					s.guidsInProgress[guid] = mc
				}
				s.gipMutex.Unlock()

				mc.mu.Lock()
				err = document.Save(pdfPath)
				if err != nil {
					mc.mu.Unlock()
					critErr = err
					s.logger.Error("Failed to generate PDF document", "guid", guid, "error", err)
					break outermost
				}
				mc.mu.Unlock()

				pdfguids[guid] = seq
			}
		}

		select {
		case <-ctx.Done():
			s.logger.Error("Context error in createPDFs", "error", ctx.Err())
			critErr = ctx.Err()
		case out <- critErr:
		}

	}()

	return out
}

func (s *Service) saveToDB(ctx context.Context, in <-chan StringLineBatch, filename string) <-chan error {
	out := make(chan error, 1)

	go func() {
		defer close(out)
		for range in {
			fmt.Println("Saving to DB from file", filename)
		}
		select {
		case <-ctx.Done():
			s.logger.Error("Context error in DB", "error", ctx.Err())
		case out <- nil:
		}
	}()
	return out
}
