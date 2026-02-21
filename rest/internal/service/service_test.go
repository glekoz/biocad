package service_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/glekoz/rest/internal/repository"
	"github.com/glekoz/rest/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- mock repo ---

type mockRepo struct {
	countByUnitGUID   func(ctx context.Context, unitGUID string) (int, error)
	getByUnitGUID     func(ctx context.Context, unitGUID string, limit, offset int) ([]repository.Record, error)
	countErroredFiles func(ctx context.Context) (int, error)
	getErroredFiles   func(ctx context.Context, limit, offset int) ([]repository.ErroredFile, error)
}

func (m *mockRepo) CountByUnitGUID(ctx context.Context, unitGUID string) (int, error) {
	return m.countByUnitGUID(ctx, unitGUID)
}
func (m *mockRepo) GetByUnitGUID(ctx context.Context, unitGUID string, limit, offset int) ([]repository.Record, error) {
	return m.getByUnitGUID(ctx, unitGUID, limit, offset)
}
func (m *mockRepo) CountErroredFiles(ctx context.Context) (int, error) {
	return m.countErroredFiles(ctx)
}
func (m *mockRepo) GetErroredFiles(ctx context.Context, limit, offset int) ([]repository.ErroredFile, error) {
	return m.getErroredFiles(ctx, limit, offset)
}

var testLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

// helpers

func ptrString(s string) *string { return &s }

var sampleRecord = repository.Record{
	ID:       1,
	N:        42,
	Invid:    "inv-1",
	UnitGuid: "unit-abc",
	MsgID:    "msg-1",
	Text:     "some text",
	Class:    "A",
	Level:    1,
	Area:     "area",
	Addr:     "addr",
}

var sampleErroredFile = repository.ErroredFile{
	ID:        1,
	Filename:  "bad.tsv",
	Error:     "parse error",
	CreatedAt: time.Now(),
}

// =================================================================
// GetRecords tests
// =================================================================

func TestGetRecords_Success(t *testing.T) {
	repo := &mockRepo{
		countByUnitGUID: func(_ context.Context, _ string) (int, error) { return 1, nil },
		getByUnitGUID: func(_ context.Context, _ string, _, _ int) ([]repository.Record, error) {
			return []repository.Record{sampleRecord}, nil
		},
	}
	svc := service.New(repo, testLogger)

	result, err := svc.GetRecords(context.Background(), "unit-abc", 1, 10)

	require.NoError(t, err)
	assert.Equal(t, 1, result.Total)
	assert.Equal(t, 1, result.Page)
	assert.Equal(t, 10, result.Limit)
	require.Len(t, result.Records, 1)
	assert.Equal(t, "unit-abc", result.Records[0].UnitGuid)
}

func TestGetRecords_Pagination_OffsetCalculated(t *testing.T) {
	var capturedOffset int
	repo := &mockRepo{
		countByUnitGUID: func(_ context.Context, _ string) (int, error) { return 25, nil },
		getByUnitGUID: func(_ context.Context, _ string, limit, offset int) ([]repository.Record, error) {
			capturedOffset = offset
			return []repository.Record{sampleRecord}, nil
		},
	}
	svc := service.New(repo, testLogger)

	_, err := svc.GetRecords(context.Background(), "unit-abc", 3, 10)
	require.NoError(t, err)
	assert.Equal(t, 20, capturedOffset) // (page-1)*limit = 2*10
}

func TestGetRecords_EmptyUnitGUID(t *testing.T) {
	svc := service.New(&mockRepo{}, testLogger)

	_, err := svc.GetRecords(context.Background(), "", 1, 10)

	assert.ErrorIs(t, err, service.ErrEmptyUnitGUID)
}

func TestGetRecords_InvalidPage(t *testing.T) {
	svc := service.New(&mockRepo{}, testLogger)

	_, err := svc.GetRecords(context.Background(), "unit-abc", 0, 10)

	assert.ErrorIs(t, err, service.ErrInvalidPage)
}

func TestGetRecords_InvalidLimit(t *testing.T) {
	svc := service.New(&mockRepo{}, testLogger)

	for _, limit := range []int{0, -1, 101} {
		_, err := svc.GetRecords(context.Background(), "unit-abc", 1, limit)
		assert.ErrorIs(t, err, service.ErrInvalidLimit, "limit=%d", limit)
	}
}

func TestGetRecords_NotFound_ZeroCount(t *testing.T) {
	repo := &mockRepo{
		countByUnitGUID: func(_ context.Context, _ string) (int, error) { return 0, nil },
	}
	svc := service.New(repo, testLogger)

	_, err := svc.GetRecords(context.Background(), "unit-abc", 1, 10)

	assert.ErrorIs(t, err, service.ErrNotFound)
}

func TestGetRecords_NotFound_EmptyRows(t *testing.T) {
	repo := &mockRepo{
		countByUnitGUID: func(_ context.Context, _ string) (int, error) { return 5, nil },
		getByUnitGUID: func(_ context.Context, _ string, _, _ int) ([]repository.Record, error) {
			return nil, repository.ErrNotFound
		},
	}
	svc := service.New(repo, testLogger)

	_, err := svc.GetRecords(context.Background(), "unit-abc", 1, 10)

	assert.ErrorIs(t, err, service.ErrNotFound)
}

func TestGetRecords_RepoError(t *testing.T) {
	repoErr := errors.New("db is down")
	repo := &mockRepo{
		countByUnitGUID: func(_ context.Context, _ string) (int, error) { return 0, repoErr },
	}
	svc := service.New(repo, testLogger)

	_, err := svc.GetRecords(context.Background(), "unit-abc", 1, 10)

	assert.ErrorIs(t, err, repoErr)
}

// =================================================================
// GetErroredFiles tests
// =================================================================

func TestGetErroredFiles_Success(t *testing.T) {
	repo := &mockRepo{
		countErroredFiles: func(_ context.Context) (int, error) { return 1, nil },
		getErroredFiles: func(_ context.Context, _, _ int) ([]repository.ErroredFile, error) {
			return []repository.ErroredFile{sampleErroredFile}, nil
		},
	}
	svc := service.New(repo, testLogger)

	result, err := svc.GetErroredFiles(context.Background(), 1, 10)

	require.NoError(t, err)
	assert.Equal(t, 1, result.Total)
	require.Len(t, result.Files, 1)
	assert.Equal(t, "bad.tsv", result.Files[0].Filename)
}

func TestGetErroredFiles_InvalidPage(t *testing.T) {
	svc := service.New(&mockRepo{}, testLogger)

	_, err := svc.GetErroredFiles(context.Background(), 0, 10)

	assert.ErrorIs(t, err, service.ErrInvalidPage)
}

func TestGetErroredFiles_InvalidLimit(t *testing.T) {
	svc := service.New(&mockRepo{}, testLogger)

	_, err := svc.GetErroredFiles(context.Background(), 1, 200)

	assert.ErrorIs(t, err, service.ErrInvalidLimit)
}

func TestGetErroredFiles_NotFound_ZeroCount(t *testing.T) {
	repo := &mockRepo{
		countErroredFiles: func(_ context.Context) (int, error) { return 0, nil },
	}
	svc := service.New(repo, testLogger)

	_, err := svc.GetErroredFiles(context.Background(), 1, 10)

	assert.ErrorIs(t, err, service.ErrNotFound)
}

func TestGetErroredFiles_RepoError(t *testing.T) {
	repoErr := errors.New("connection refused")
	repo := &mockRepo{
		countErroredFiles: func(_ context.Context) (int, error) { return 0, repoErr },
	}
	svc := service.New(repo, testLogger)

	_, err := svc.GetErroredFiles(context.Background(), 1, 10)

	assert.ErrorIs(t, err, repoErr)
}
