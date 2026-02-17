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
		s.fipMutex.Lock()
		s.filesInProgress[filename] = true
		s.fipMutex.Unlock()

		s.semaphore <- struct{}{}
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
			errChan1 := s.createPDFs(iterCtx, tsvChan1, f)
			errChan2 := s.saveToDB(iterCtx, tsvChan2, f)

			// в данном случае поддерживаю консистентное состояние, при котором
			// файл считается обработанным, только если оба канала ошибок вернули nil
			var finalErr error
			for range 2 {
				select {
				case err := <-errChan1:
					if err != nil {
						s.logger.Error("Failed to create PDFs", "file", f, "error", err)
						// move(s.SourcePath, s.ErrorPath, f) // перемещаю файл в папку с ошибками
						finalErr = err
						return
					}
					errChan1 = nil
				case err := <-errChan2:
					if err != nil {
						s.logger.Error("Failed to save to DB", "file", f, "error", err)
						finalErr = err
						return
					}
					errChan2 = nil
				case <-ctx.Done():
					s.logger.Error("Processing timed out", "file", f)
					finalErr = ctx.Err()
					return
				}
			}

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
