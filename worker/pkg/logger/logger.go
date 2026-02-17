package logger

import (
	"context"
	"io"
	"log/slog"
	"time"
)

type customKey int

const (
	LogDataKey customKey = iota
)

type MyJSONLogHandler struct {
	handler slog.Handler
}

type LogData struct {
	Filename string
	Details  map[string]any
}

func New(w io.Writer, opts *slog.HandlerOptions) *slog.Logger {
	if opts != nil {
		return slog.New(NewMyJSONLogHandler(slog.Handler(slog.NewJSONHandler(w, opts))))
	}
	handler := slog.Handler(slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.TimeValue(a.Value.Time().Truncate(time.Minute))
			}
			return a
		},
		Level: slog.LevelInfo,
	}))
	handler = NewMyJSONLogHandler(handler)
	return slog.New(handler)
}

func NewMyJSONLogHandler(h slog.Handler) *MyJSONLogHandler {
	return &MyJSONLogHandler{handler: h}
}

// В секции ниже добавляю методы к моей структуре, чтобы она удовлетворяла
// интерфейсу slog.Handler

func (h *MyJSONLogHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.handler.Enabled(ctx, lvl)
}

func (h *MyJSONLogHandler) Handle(ctx context.Context, rec slog.Record) error {
	if ld, ok := ctx.Value(LogDataKey).(LogData); ok {
		if ld.Filename != "" {
			rec.Add("filename", ld.Filename)
		}
		if ld.Details != nil {
			rec.Add("details", ld.Details)
		}
	}
	return h.handler.Handle(ctx, rec)
}

func (h *MyJSONLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h.handler.WithAttrs(attrs)
}

func (h *MyJSONLogHandler) WithGroup(name string) slog.Handler {
	return h.handler.WithGroup(name)
}

// Ниже находятся функции для удобного добавления данных
// в контекст логгера

func WithFilename(ctx context.Context, filename string) context.Context {
	if ld, ok := ctx.Value(LogDataKey).(LogData); ok {
		ld.Filename = filename
		return context.WithValue(ctx, LogDataKey, ld)
	}
	return context.WithValue(ctx, LogDataKey, LogData{Filename: filename})
}

// это в основном для дополнительных данных во время ошибок
func WithDetails(ctx context.Context, key string, detail any) context.Context {
	if ld, ok := ctx.Value(LogDataKey).(LogData); ok {
		if ld.Details == nil {
			ld.Details = make(map[string]any)
		}
		ld.Details[key] = detail
		return context.WithValue(ctx, LogDataKey, ld)
	}
	return context.WithValue(ctx, LogDataKey, LogData{Details: map[string]any{key: detail}})
}
