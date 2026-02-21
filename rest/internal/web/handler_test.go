package web_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"log/slog"
	"os"

	api "github.com/glekoz/rest/api/v1"
	"github.com/glekoz/rest/internal/models"
	"github.com/glekoz/rest/internal/service"
	"github.com/glekoz/rest/internal/web"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- mock service ---

type mockService struct {
	getRecords      func(ctx context.Context, unitGUID string, page, limit int) (models.PaginatedRecords, error)
	getErroredFiles func(ctx context.Context, page, limit int) (models.PaginatedErroredFiles, error)
}

func (m *mockService) GetRecords(ctx context.Context, unitGUID string, page, limit int) (models.PaginatedRecords, error) {
	return m.getRecords(ctx, unitGUID, page, limit)
}
func (m *mockService) GetErroredFiles(ctx context.Context, page, limit int) (models.PaginatedErroredFiles, error) {
	return m.getErroredFiles(ctx, page, limit)
}

var testLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

// newServer собирает готовый http.Handler для тестов
func newServer(svc *mockService) http.Handler {
	h := web.NewHandler(svc, testLogger)
	return web.NewServer(h)
}

func doRequest(t *testing.T, srv http.Handler, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, url, nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

// =================================================================
// GET /api/v1/{unit_guid}
// =================================================================

func TestGetRecords_200(t *testing.T) {
	mqtt := "mqtt-val"
	svc := &mockService{
		getRecords: func(_ context.Context, unitGUID string, page, limit int) (models.PaginatedRecords, error) {
			assert.Equal(t, "abc-123", unitGUID)
			assert.Equal(t, 1, page)
			assert.Equal(t, 10, limit)
			return models.PaginatedRecords{
				Page:  1,
				Limit: 10,
				Total: 1,
				Records: []models.Record{{
					ID:        1,
					N:         5,
					Mqtt:      &mqtt,
					Invid:     "inv",
					UnitGuid:  "abc-123",
					MsgID:     "msg",
					Text:      "text",
					Class:     "A",
					Level:     1,
					Area:      "area",
					Addr:      "addr",
					CreatedAt: time.Now(),
				}},
			}, nil
		},
	}

	rr := doRequest(t, newServer(svc), http.MethodGet, "/api/v1/records/abc-123")

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var resp api.RecordsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 1, resp.Pagination.Total)
	assert.Equal(t, 1, resp.Pagination.Page)
	require.Len(t, resp.Records, 1)
	assert.Equal(t, "abc-123", resp.Records[0].UnitGuid)
	assert.Equal(t, &mqtt, resp.Records[0].Mqtt)
}

func TestGetRecords_PageAndLimit_QueryParams(t *testing.T) {
	var capturedPage, capturedLimit int
	svc := &mockService{
		getRecords: func(_ context.Context, _ string, page, limit int) (models.PaginatedRecords, error) {
			capturedPage = page
			capturedLimit = limit
			return models.PaginatedRecords{Page: page, Limit: limit, Total: 5, Records: []models.Record{}}, nil
		},
	}

	doRequest(t, newServer(svc), http.MethodGet, "/api/v1/records/unit-1?page=2&limit=5")

	assert.Equal(t, 2, capturedPage)
	assert.Equal(t, 5, capturedLimit)
}

func TestGetRecords_404(t *testing.T) {
	svc := &mockService{
		getRecords: func(_ context.Context, _ string, _, _ int) (models.PaginatedRecords, error) {
			return models.PaginatedRecords{}, service.ErrNotFound
		},
	}

	rr := doRequest(t, newServer(svc), http.MethodGet, "/api/v1/records/unknown")

	assert.Equal(t, http.StatusNotFound, rr.Code)
	var resp api.ErrorResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.NotEmpty(t, resp.Error)
}

func TestGetRecords_400_InvalidLimit(t *testing.T) {
	svc := &mockService{
		getRecords: func(_ context.Context, _ string, _, _ int) (models.PaginatedRecords, error) {
			return models.PaginatedRecords{}, service.ErrInvalidLimit
		},
	}

	rr := doRequest(t, newServer(svc), http.MethodGet, "/api/v1/records/unit-1?limit=999")

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetRecords_500_InternalError(t *testing.T) {
	svc := &mockService{
		getRecords: func(_ context.Context, _ string, _, _ int) (models.PaginatedRecords, error) {
			return models.PaginatedRecords{}, errors.New("unexpected db error")
		},
	}

	rr := doRequest(t, newServer(svc), http.MethodGet, "/api/v1/records/unit-1")

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	var resp api.ErrorResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "internal server error", resp.Error)
}

// =================================================================
// GET /api/v1/errors
// =================================================================

func TestGetErroredFiles_200(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	svc := &mockService{
		getErroredFiles: func(_ context.Context, page, limit int) (models.PaginatedErroredFiles, error) {
			assert.Equal(t, 1, page)
			assert.Equal(t, 10, limit)
			return models.PaginatedErroredFiles{
				Page:  1,
				Limit: 10,
				Total: 1,
				Files: []models.ErroredFile{{
					ID:        7,
					Filename:  "broken.tsv",
					Error:     "parse error on line 3",
					CreatedAt: now,
				}},
			}, nil
		},
	}

	rr := doRequest(t, newServer(svc), http.MethodGet, "/api/v1/errors")

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp api.ErroredFilesResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 1, resp.Pagination.Total)
	require.Len(t, resp.Files, 1)
	assert.Equal(t, "broken.tsv", resp.Files[0].Filename)
	assert.Equal(t, "parse error on line 3", resp.Files[0].Error)
}

func TestGetErroredFiles_PageAndLimit_QueryParams(t *testing.T) {
	var capturedPage, capturedLimit int
	svc := &mockService{
		getErroredFiles: func(_ context.Context, page, limit int) (models.PaginatedErroredFiles, error) {
			capturedPage = page
			capturedLimit = limit
			return models.PaginatedErroredFiles{Page: page, Limit: limit, Total: 3, Files: []models.ErroredFile{}}, nil
		},
	}

	doRequest(t, newServer(svc), http.MethodGet, "/api/v1/errors?page=3&limit=20")

	assert.Equal(t, 3, capturedPage)
	assert.Equal(t, 20, capturedLimit)
}

func TestGetErroredFiles_404(t *testing.T) {
	svc := &mockService{
		getErroredFiles: func(_ context.Context, _, _ int) (models.PaginatedErroredFiles, error) {
			return models.PaginatedErroredFiles{}, service.ErrNotFound
		},
	}

	rr := doRequest(t, newServer(svc), http.MethodGet, "/api/v1/errors")

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetErroredFiles_500_InternalError(t *testing.T) {
	svc := &mockService{
		getErroredFiles: func(_ context.Context, _, _ int) (models.PaginatedErroredFiles, error) {
			return models.PaginatedErroredFiles{}, errors.New("connection refused")
		},
	}

	rr := doRequest(t, newServer(svc), http.MethodGet, "/api/v1/errors")

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	var resp api.ErrorResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "internal server error", resp.Error)
}
