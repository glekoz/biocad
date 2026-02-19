package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/glekoz/biocad/worker/config"
	"github.com/glekoz/biocad/worker/internal/models"
	"github.com/glekoz/biocad/worker/internal/service/helper"
	"github.com/glekoz/biocad/worker/internal/service/pdf"
	"github.com/glekoz/biocad/worker/pkg/logger"
	"github.com/rs/xid"
)

type MutexCounter struct {
	mu    *sync.Mutex
	count int
}

type RepoAPI interface {
	BeginTx(ctx context.Context) (TxAPI, error)
	SaveErroredFile(ctx context.Context, filename string, errText string) error
}

type TxAPI interface {
	InsertRecord(ctx context.Context, rec models.TSVRecord) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
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
			errChanDB := s.saveToDB(iterCtx, tsvChan2, filename)

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
						finalErr = err
					}
					errChanDB = nil
				case err := <-errChanPDF:
					if err != nil {
						finalErr = err
					}
					errChanPDF = nil
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

			s.logger.InfoContext(iterCtx, "Successfully processed file")
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
			if batch.Err != nil {
				critErr = batch.Err
				break
			}
			for i, rec := range batch.Records {
				if i%(s.BatchSize>>1) == 0 {
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
		case out <- critErr:
		}
	}()
	return out
}

func (s *Service) saveToDB(ctx context.Context, in <-chan StringLineBatch, filename string) <-chan error {
	out := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("Panic recovered in saveToDB: %v", r)
			}
			close(out)
		}()

		if s.repo == nil {
			select {
			case <-ctx.Done():
			case out <- errors.New("repository is not initialized"):
			}
			return
		}

		tx, err := s.repo.BeginTx(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
			case out <- err:
			}
			return
		}

		committed := false
		defer func() {
			if committed {
				return
			}
			rbErr := tx.Rollback(context.WithoutCancel(ctx))
			if rbErr != nil {
				s.logger.ErrorContext(ctx, "Failed to rollback transaction", "error", rbErr)
			}
		}()

		var finalErr error
		checkEvery := s.BatchSize >> 1
		if checkEvery == 0 {
			checkEvery = 1
		}
	outer:
		for batch := range in {
			if batch.Err != nil {
				finalErr = batch.Err
				break
			}

			for i, rawRecord := range batch.Records {
				if i%checkEvery == 0 {
					if ctx.Err() != nil {
						finalErr = ctx.Err()
						break outer
					}
				}

				rec, parseErr := mapRawRecordToTSV(rawRecord)
				if parseErr != nil {
					finalErr = parseErr
					break outer
				}

				if err = tx.InsertRecord(ctx, rec); err != nil {
					finalErr = err
					break outer
				}
			}
		}

		if finalErr == nil {
			err = tx.Commit(ctx)
			if err != nil {
				finalErr = err
			} else {
				committed = true
			}
		}

		if finalErr != nil {
			saveErr := s.repo.SaveErroredFile(context.WithoutCancel(ctx), filename, finalErr.Error())
			if saveErr != nil {
				s.logger.ErrorContext(ctx, "Failed to save errored file metadata", "error", saveErr, "filename", filename)
			}
			select {
			case <-ctx.Done():
			case out <- finalErr:
			}
			return
		}

		select {
		case <-ctx.Done():
			out <- ctx.Err()
		case out <- nil:
		}
	}()
	return out
}

func mapRawRecordToTSV(raw []string) (models.TSVRecord, error) {
	if len(raw) != 15 {
		return models.TSVRecord{}, fmt.Errorf("invalid record size: got %d, expected 15", len(raw))
	}

	n, err := strconv.Atoi(raw[0])
	if err != nil {
		return models.TSVRecord{}, fmt.Errorf("invalid n value %q: %w", raw[0], err)
	}

	level, err := strconv.Atoi(raw[8])
	if err != nil {
		return models.TSVRecord{}, fmt.Errorf("invalid level value %q: %w", raw[8], err)
	}

	return models.TSVRecord{
		N:         n,
		MQTT:      raw[1],
		InvID:     raw[2],
		UnitGUID:  raw[3],
		MsgID:     raw[4],
		Text:      raw[5],
		Context:   raw[6],
		Class:     raw[7],
		Level:     level,
		Area:      raw[9],
		Addr:      raw[10],
		Block:     raw[11],
		Type:      raw[12],
		Bit:       raw[13],
		InvertBit: raw[14],
	}, nil
}
