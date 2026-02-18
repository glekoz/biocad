package service

import (
	"context"
	"sync"
	"time"
)

// func (s *Service) processFile(ctx context.Context, filename string) {
// 	// parse file
// 	// create pdf files
// 	// save to Postgres and delete from s.filesInProgress and move file to completedPath
// }

func (s *Service) ProcessFiles(ctx context.Context, filenames <-chan string) {
	wg := &sync.WaitGroup{}
	for filename := range filenames {
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
