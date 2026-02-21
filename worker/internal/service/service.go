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
	"github.com/glekoz/biocad/worker/internal/repository"
	"github.com/glekoz/biocad/worker/internal/service/helper"
	"github.com/glekoz/biocad/worker/internal/service/pdf"
	"github.com/glekoz/biocad/worker/pkg/logger"
	"github.com/rs/xid"
)

type MutexCounter struct {
	mu    *sync.Mutex
	count int
}

type Service struct {
	SourcePath                   string
	CompletedPath                string
	ErrorPath                    string
	PDFPath                      string
	PDFConfig                    *pdf.Config
	PollIntervalMs               int
	MaxWorkers                   int
	BatchSize                    int
	FileProcessingTimeoutSeconds int

	logger    *slog.Logger
	repo      RepoAPI
	semaphore chan struct{}
	parser    *Parser

	filesInProgress map[string]bool
	fipMutex        *sync.RWMutex

	guidsInProgress map[string]*MutexCounter // для блокировки работы с директориями [guid]mu
	gipMutex        *sync.Mutex
}

type RepoAPI interface {
	BeginTx(ctx context.Context) (repository.Tx, error)
	SaveErroredFile(ctx context.Context, filename string, errText string) error
}

func New(cfg config.Worker, log *slog.Logger, r RepoAPI) (*Service, error) {
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
		SourcePath:                   cfg.FromPath,
		CompletedPath:                completedPath,
		ErrorPath:                    errorPath,
		PDFPath:                      pdfPath,
		PDFConfig:                    pdfConf,
		PollIntervalMs:               cfg.PollIntervalMs,
		MaxWorkers:                   cfg.MaxWorkers,
		BatchSize:                    cfg.BatchSize,
		FileProcessingTimeoutSeconds: cfg.FileProcessingTimeoutSeconds,

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

func (s *Service) Run(ctx context.Context) {
	s.logger.Info("Service is running", "fromPath", s.SourcePath, "pollInterval", s.PollIntervalMs, "maxWorkers", s.MaxWorkers, "batchSize", s.BatchSize)

	fileStream := s.streamFiles(ctx)

	wg := &sync.WaitGroup{}
	for filename := range fileStream {
		s.semaphore <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() {
				r := recover()
				if r != nil {
					s.logger.Error("Panic recovered in .Run: %v", r)
				}
				<-s.semaphore
				wg.Done()
			}()

			s.fipMutex.Lock()
			s.filesInProgress[filename] = true
			s.fipMutex.Unlock()

			iterCtx, iterCancel := context.WithTimeout(ctx, time.Duration(s.FileProcessingTimeoutSeconds)*time.Second)
			defer iterCancel()
			iterCtx = logger.WithFilename(iterCtx, filename)

			tsvChan1, tsvChan2, err := s.streamTSV(iterCtx, filename)
			if err != nil {
				s.logger.ErrorContext(iterCtx, "Failed to parse file", "error", err)
				return
			}
			errChanPDF := s.createPDFs(iterCtx, tsvChan1)
			errChanDB := s.saveToDB(iterCtx, tsvChan2)

			// в данном случае поддерживаю консистентное состояние, при котором
			// файл считается обработанным, только если оба канала ошибок вернули nil
			// сначала жду ошибку из канала сохранения в БД, так как это более критичная операция, и если она не удалась, то нет смысла создавать PDF
			var finalErr error
			// так как прием значений из канала не гарантирует, что горутина, которая его пишет, уже завершилась,
			// то нужно дождаться закрытия канала, чтобы убедиться, что все операции по сохранению в БД и созданию PDF завершены
			ecpdf, ecdb := errChanPDF, errChanDB
			for range 2 {
				if finalErr != nil {
					iterCancel()
					break
				}
				select {
				case err := <-ecdb:
					if err != nil {
						finalErr = err
					}
					ecdb = nil
				case err := <-ecpdf:
					if err != nil {
						finalErr = err
					}
					ecpdf = nil
				case <-iterCtx.Done():
					finalErr = iterCtx.Err()
				}
			}

			// на этом этапе файл уже обработан, поэтому
			// если произошла ошибка при перемещении файла,
			// то он остается в мапе и не будет обрабатываться повторно
			if finalErr != nil {
				if errors.Is(finalErr, ErrInvalidFileFormat) {
					s.logger.WarnContext(logger.ErrorCtx(iterCtx, finalErr), "Input error", "error", finalErr)
				} else {
					s.logger.ErrorContext(logger.ErrorCtx(iterCtx, finalErr), "System error", "error", finalErr)
				}
				err = s.repo.SaveErroredFile(context.WithoutCancel(ctx), filename, finalErr.Error())
				if err != nil {
					s.logger.ErrorContext(ctx, "Ошибка сохранения информации об ошибочном файле", "error", err)
				}
				err = helper.Move(s.SourcePath, s.ErrorPath, filename)
				if err != nil {
					s.logger.ErrorContext(iterCtx, "Failed to move file to errors directory", "error", err)
					return
				}
			} else {
				err = helper.Move(s.SourcePath, s.CompletedPath, filename)
				if err != nil {
					s.logger.ErrorContext(iterCtx, "Failed to move file to completed directory", "error", err)
					return
				}
			}

			s.fipMutex.Lock()
			delete(s.filesInProgress, filename)
			s.fipMutex.Unlock()

			// жду закрытия каналов для корректной очистки ресурсов в defer
			for range errChanPDF {
			}
			for range errChanDB {
			}

			s.logger.InfoContext(iterCtx, "Processed file")
		}()
	}
	wg.Wait()
}

func (s *Service) createPDFs(ctx context.Context, in <-chan StringLineBatch) <-chan error {
	out := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("Panic recovered in createPDFs: %v", r)
			}
			close(out)
		}()

		var critErr error
		pdfguids := make(map[string]string) // ключ - guid, значение - xid (имя создаваемого pdf файла), которые добавляются из текущего обрабатываемого файла
		recordsByID := make(map[string][][]string)

		defer func() {
			if critErr != nil {
				for guid, seq := range pdfguids {
					dir := filepath.Join(s.PDFPath, guid)
					if seq != "" {
						p := filepath.Join(dir, seq+".pdf")
						err := os.Remove(p)
						if err != nil {
							s.logger.Error("Ошибка удаления PDF файла во время очистки", "file", p, "error", err)
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
						s.logger.Error("Ошибка чтения директории с PDF во время очистки", "dir", dir, "error", err)
					} else if len(entries) == 0 {
						err = os.Remove(dir)
						if err != nil {
							s.logger.Error("Ошибка удаления директории с PDF во время очистки", "dir", dir, "error", err)
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

		checkEvery := s.BatchSize >> 1
		if checkEvery == 0 {
			checkEvery = 1
		}

	outermost:
		for batch := range in {
			if batch.Err != nil {
				critErr = batch.Err
				break
			}
			for i, rec := range batch.Records {
				if i%checkEvery == 0 {
					if ctx.Err() != nil {
						critErr = ctx.Err()
						break outermost
					}
				}
				if recordsByID[rec[3]] == nil {
					recordsByID[rec[3]] = [][]string{rec}
				} else {
					recordsByID[rec[3]] = append(recordsByID[rec[3]], rec)
				}
			}
		}
		for guid, recs := range recordsByID {
			ctx = logger.WithDetails(ctx, "guid", guid)
			if ctx.Err() != nil {
				critErr = logger.WrapError(ctx, ctx.Err())
				break
			}

			seq := xid.New().String()
			dir := filepath.Join(s.PDFPath, guid)
			err := os.MkdirAll(dir, 0755)
			if err != nil {
				ctx = logger.WithDetails(ctx, "pdf dir", dir)
				critErr = logger.WrapError(ctx, err)
				break
			}

			// пустая строка нужна, чтобы появилась запись в мапе
			// чтобы удалить лишнюю директорию в случае ошибки, так как она создается до генерации PDF
			pdfguids[guid] = ""
			pdfPath := filepath.Join(dir, seq+".pdf")

			pdfer := pdf.NewHandler(s.PDFConfig)
			pdfer.AddTitleAndHeader(fmt.Sprintf("Results for guid %s from file %s", guid, helper.GetFilenameFromContext(ctx)))
			pdfer.AddDataRows(recs)
			document, err := pdfer.Generate()
			if err != nil {
				critErr = logger.WrapError(ctx, err)
				break
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
				critErr = logger.WrapError(ctx, err)
				break
			}
			mc.mu.Unlock()

			pdfguids[guid] = seq
		}

		select {
		case <-ctx.Done():
			critErr = ctx.Err()
		case out <- critErr:
		}
	}()
	return out
}

func (s *Service) saveToDB(ctx context.Context, in <-chan StringLineBatch) <-chan error {
	out := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("Panic recovered in saveToDB: %v", r)
			}
			close(out)
		}()

		tx, err := s.repo.BeginTx(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
			case out <- err:
			}
			return
		}
		defer tx.Rollback(context.WithoutCancel(ctx))

		var critErr error

		checkEvery := s.BatchSize >> 1
		if checkEvery == 0 {
			checkEvery = 1
		}

		for batch := range in {
			if batch.Err != nil {
				critErr = batch.Err
				break
			}

			err = tx.InsertRecordsBatch(ctx, batch.Records)
			if err != nil {
				critErr = err
				break
			}
		}
		if critErr == nil {
			critErr = tx.Commit(ctx)
		}
		if critErr != nil {
			err = tx.Rollback(context.WithoutCancel(ctx))
			if err != nil {
				s.logger.ErrorContext(ctx, "Failed to rollback transaction", "error", err)
			}
		}

		select {
		case <-ctx.Done():
		case out <- critErr:
		}
	}()
	return out
}
