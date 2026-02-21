package web

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	api "github.com/glekoz/rest/api/v1"
	"github.com/glekoz/rest/internal/models"
	"github.com/glekoz/rest/internal/service"
)

type ServiceAPI interface {
	GetRecords(ctx context.Context, unitGUID string, page, limit int) (models.PaginatedRecords, error)
}

type Handler struct {
	svc    ServiceAPI
	logger *slog.Logger
}

func NewHandler(svc ServiceAPI, logger *slog.Logger) *Handler {
	return &Handler{svc: svc, logger: logger}
}

// GetRecords реализует интерфейс api.ServerInterface.
// unitGuid — path-параметр /api/v1/{unit_guid}, params — query-параметры page и limit.
func (h *Handler) GetRecords(w http.ResponseWriter, r *http.Request, unitGuid string, params api.GetRecordsParams) {
	page := 1
	if params.Page != nil {
		page = *params.Page
	}

	limit := 10
	if params.Limit != nil {
		limit = *params.Limit
	}

	result, err := h.svc.GetRecords(r.Context(), unitGuid, page, limit)
	if err != nil {
		h.handleError(w, err)
		return
	}

	resp := api.RecordsResponse{
		Records: make([]api.Record, 0, len(result.Records)),
		Pagination: api.Pagination{
			Page:  result.Page,
			Limit: result.Limit,
			Total: result.Total,
		},
	}

	for _, rec := range result.Records {
		resp.Records = append(resp.Records, api.Record{
			Id:        rec.ID,
			N:         rec.N,
			Mqtt:      rec.Mqtt,
			Invid:     rec.Invid,
			UnitGuid:  rec.UnitGuid,
			MsgId:     rec.MsgID,
			Text:      rec.Text,
			Context:   rec.Context,
			Class:     rec.Class,
			Level:     rec.Level,
			Area:      rec.Area,
			Addr:      rec.Addr,
			Block:     rec.Block,
			Type:      rec.Type,
			Bit:       rec.Bit,
			InvertBit: rec.InvertBit,
			CreatedAt: rec.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrEmptyUnitGUID),
		errors.Is(err, service.ErrInvalidPage),
		errors.Is(err, service.ErrInvalidLimit):
		writeJSON(w, http.StatusBadRequest, api.ErrorResponse{Error: err.Error()})

	case errors.Is(err, service.ErrNotFound):
		writeJSON(w, http.StatusNotFound, api.ErrorResponse{Error: err.Error()})

	default:
		h.logger.Error("internal error", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, api.ErrorResponse{Error: "internal server error"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

// NewServer создаёт http.Handler с маршрутами из OpenAPI спецификации.
// Производит регистрацию маршрута: GET /api/v1/{unit_guid}
func NewServer(handler *Handler) http.Handler {
	return api.Handler(handler)
}
