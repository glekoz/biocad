package service

import (
	"context"
	"errors"
	"log/slog"

	"github.com/glekoz/rest/internal/models"
	"github.com/glekoz/rest/internal/repository"
)

type RepoAPI interface {
	GetByUnitGUID(ctx context.Context, unitGUID string, limit, offset int) ([]repository.Record, error)
	CountByUnitGUID(ctx context.Context, unitGUID string) (int, error)
	GetErroredFiles(ctx context.Context, limit, offset int) ([]repository.ErroredFile, error)
	CountErroredFiles(ctx context.Context) (int, error)
}

type Service struct {
	repo   RepoAPI
	logger *slog.Logger
}

func New(repo RepoAPI, logger *slog.Logger) *Service {
	return &Service{repo: repo, logger: logger}
}

func (s *Service) GetRecords(ctx context.Context, unitGUID string, page, limit int) (models.PaginatedRecords, error) {
	if unitGUID == "" {
		return models.PaginatedRecords{}, ErrEmptyUnitGUID
	}
	if page < 1 {
		return models.PaginatedRecords{}, ErrInvalidPage
	}
	if limit < 1 || limit > 100 {
		return models.PaginatedRecords{}, ErrInvalidLimit
	}

	offset := (page - 1) * limit

	total, err := s.repo.CountByUnitGUID(ctx, unitGUID)
	if err != nil {
		s.logger.ErrorContext(ctx, "count records failed", slog.String("error", err.Error()))
		return models.PaginatedRecords{}, err
	}

	if total == 0 {
		return models.PaginatedRecords{}, ErrNotFound
	}

	repoRecords, err := s.repo.GetByUnitGUID(ctx, unitGUID, limit, offset)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return models.PaginatedRecords{}, ErrNotFound
		}
		s.logger.ErrorContext(ctx, "get records failed", slog.String("error", err.Error()))
		return models.PaginatedRecords{}, err
	}

	records := make([]models.Record, 0, len(repoRecords))
	for _, r := range repoRecords {
		records = append(records, models.Record{
			ID:        int(r.ID),
			N:         int(r.N),
			Mqtt:      r.Mqtt,
			Invid:     r.Invid,
			UnitGuid:  r.UnitGuid,
			MsgID:     r.MsgID,
			Text:      r.Text,
			Context:   r.Context,
			Class:     r.Class,
			Level:     int(r.Level),
			Area:      r.Area,
			Addr:      r.Addr,
			Block:     r.Block,
			Type:      r.Type,
			Bit:       r.Bit,
			InvertBit: r.InvertBit,
			CreatedAt: r.CreatedAt,
		})
	}

	return models.PaginatedRecords{
		Records: records,
		Page:    page,
		Limit:   limit,
		Total:   total,
	}, nil
}

func (s *Service) GetErroredFiles(ctx context.Context, page, limit int) (models.PaginatedErroredFiles, error) {
	if page < 1 {
		return models.PaginatedErroredFiles{}, ErrInvalidPage
	}
	if limit < 1 || limit > 100 {
		return models.PaginatedErroredFiles{}, ErrInvalidLimit
	}

	offset := (page - 1) * limit

	total, err := s.repo.CountErroredFiles(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "count errored files failed", slog.String("error", err.Error()))
		return models.PaginatedErroredFiles{}, err
	}

	if total == 0 {
		return models.PaginatedErroredFiles{}, ErrNotFound
	}

	repoFiles, err := s.repo.GetErroredFiles(ctx, limit, offset)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return models.PaginatedErroredFiles{}, ErrNotFound
		}
		s.logger.ErrorContext(ctx, "get errored files failed", slog.String("error", err.Error()))
		return models.PaginatedErroredFiles{}, err
	}

	files := make([]models.ErroredFile, len(repoFiles))
	for i, f := range repoFiles {
		files[i] = models.ErroredFile{
			ID:        int(f.ID),
			Filename:  f.Filename,
			Error:     f.Error,
			CreatedAt: f.CreatedAt,
		}
	}

	return models.PaginatedErroredFiles{
		Files: files,
		Page:  page,
		Limit: limit,
		Total: total,
	}, nil
}
