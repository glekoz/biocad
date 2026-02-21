package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/glekoz/rest/config"
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

type ErroredFile struct {
	ID        int32
	Filename  string
	Error     string
	CreatedAt time.Time
}

type Repository struct {
	q    *db.Queries
	pool *pgxpool.Pool
}

func NewPool(ctx context.Context, config config.Config) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", config.PG.User, config.PG.Password, config.PG.Host, config.PG.Port, config.PG.DBName, config.PG.SSLMode)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.PingTimeout = 30 * time.Second
	poolCfg.MaxConns = int32(config.PG.PoolMax)
	poolCfg.MinConns = 2
	poolCfg.HealthCheckPeriod = 1 * time.Minute
	p, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	err = p.Ping(ctx)
	if err != nil {
		p.Close()
		return nil, err
	}

	return p, nil
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

func (r *Repository) CountErroredFiles(ctx context.Context) (int, error) {
	count, err := r.q.CountErroredFiles(ctx)
	if err != nil {
		return 0, fmt.Errorf("count errored files: %w", err)
	}
	return int(count), nil
}

func (r *Repository) GetErroredFiles(ctx context.Context, limit, offset int) ([]ErroredFile, error) {
	rows, err := r.q.GetErroredFiles(ctx, db.GetErroredFilesParams{
		Limit:  int32(limit),
		Offset: int32(offset),
	})
	if err != nil {
		return nil, fmt.Errorf("query errored files: %w", err)
	}

	if len(rows) == 0 {
		return nil, ErrNotFound
	}

	files := make([]ErroredFile, len(rows))
	for i, row := range rows {
		files[i] = ErroredFile{
			ID:        row.ID,
			Filename:  row.Filename,
			Error:     row.Error,
			CreatedAt: row.CreatedAt,
		}
	}
	return files, nil
}

func nullableText(t pgtype.Text) *string {
	if !t.Valid {
		return nil
	}
	return &t.String
}
