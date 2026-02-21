package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/glekoz/rest/internal/repository/db"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Record struct {
	ID        int32
	N         int32
	Mqtt      *string
	Invid     string
	UnitGuid  string
	MsgID     string
	Text      string
	Context   *string
	Class     string
	Level     int32
	Area      string
	Addr      string
	Block     *string
	Type      *string
	Bit       *string
	InvertBit *string
	CreatedAt time.Time
}

type Repository struct {
	q    *db.Queries
	pool *pgxpool.Pool
}

func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.PingTimeout = 30 * time.Second
	poolCfg.MaxConns = 10
	poolCfg.MinConns = 2
	poolCfg.HealthCheckPeriod = 1 * time.Minute

	return pgxpool.NewWithConfig(ctx, poolCfg)
}

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{
		q:    db.New(pool),
		pool: pool,
	}
}

func (r *Repository) CountByUnitGUID(ctx context.Context, unitGUID string) (int, error) {
	count, err := r.q.CountByUnitGUID(ctx, unitGUID)
	if err != nil {
		return 0, fmt.Errorf("count records: %w", err)
	}
	return int(count), nil
}

func (r *Repository) GetByUnitGUID(ctx context.Context, unitGUID string, limit, offset int) ([]Record, error) {
	rows, err := r.q.GetByUnitGUID(ctx, db.GetByUnitGUIDParams{
		UnitGuid: unitGUID,
		Limit:    int32(limit),
		Offset:   int32(offset),
	})
	if err != nil {
		return nil, fmt.Errorf("query records: %w", err)
	}

	if len(rows) == 0 {
		return nil, ErrNotFound
	}

	records := make([]Record, len(rows))
	for i, row := range rows {
		records[i] = Record{
			ID:        row.ID,
			N:         row.N,
			Mqtt:      nullableText(row.Mqtt),
			Invid:     row.Invid,
			UnitGuid:  row.UnitGuid,
			MsgID:     row.MsgID,
			Text:      row.Text,
			Context:   nullableText(row.Context),
			Class:     row.Class,
			Level:     row.Level,
			Area:      row.Area,
			Addr:      row.Addr,
			Block:     nullableText(row.Block),
			Type:      nullableText(row.Type),
			Bit:       nullableText(row.Bit),
			InvertBit: nullableText(row.InvertBit),
			CreatedAt: row.CreatedAt,
		}
	}
	return records, nil
}

// Close закрывает пул соединений
func (r *Repository) Close() {
	r.pool.Close()
}

// Ping проверяет доступность БД
func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func nullableText(t pgtype.Text) *string {
	if !t.Valid {
		return nil
	}
	return &t.String
}
