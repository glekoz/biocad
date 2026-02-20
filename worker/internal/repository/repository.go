package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/glekoz/biocad/worker/config"
	"github.com/glekoz/biocad/worker/internal/repository/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	InsertRecordsBatch(ctx context.Context, records [][]string) error
}

type Repository struct {
	q    *db.Queries
	pool *pgxpool.Pool
}

type Transaction struct {
	tx pgx.Tx
}

func NewPool(ctx context.Context, config config.Config) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", config.PG.User, config.PG.Password, config.PG.Host, config.PG.Port, config.PG.DBName, config.PG.SSLMode)

	// ccfg, err := pgx.ParseConfig(dsn)
	// if err != nil {
	// 	return nil, err
	// }
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.PingTimeout = 30 * time.Second
	poolCfg.MaxConns = int32(config.PG.PoolMax)
	poolCfg.MinConns = 4
	poolCfg.HealthCheckPeriod = 1 * time.Minute

	return pgxpool.NewWithConfig(ctx, poolCfg)
}

func New(ctx context.Context, p *pgxpool.Pool) *Repository {
	q := db.New(p)
	return &Repository{
		q:    q,
		pool: p,
	}
}

func (r *Repository) SaveErroredFile(ctx context.Context, filename string, errText string) error {
	return r.q.InsertErroredFile(ctx, db.InsertErroredFileParams{
		Filename: filename,
		Error:    errText,
	})
}

func (r *Repository) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &Transaction{tx: tx}, nil
}

func (t *Transaction) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *Transaction) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

func (t *Transaction) InsertRecordsBatch(ctx context.Context, records [][]string) error {
	if len(records) == 0 {
		return nil
	}

	rows := make([][]any, 0, len(records))
	for _, rec := range records {
		rows = append(rows, []any{
			rec[0],                  // n
			nullableString(rec[1]),  // mqtt
			rec[2],                  // inv_id
			rec[3],                  // unit_guid
			rec[4],                  // msg_id
			nullableString(rec[5]),  // text
			nullableString(rec[6]),  // context
			nullableString(rec[7]),  // class
			rec[8],                  // level
			nullableString(rec[9]),  // area
			nullableString(rec[10]), // addr
			nullableString(rec[11]), // block
			nullableString(rec[12]), // type
			nullableString(rec[13]), // bit
			nullableString(rec[14]), // invert_bit
		})
	}

	_, err := t.tx.CopyFrom(
		ctx,
		pgx.Identifier{"records"},
		[]string{
			"n", "mqtt", "invid", "unit_guid", "msg_id",
			"text", "context", "class", "level", "area",
			"addr", "block", "type", "bit", "invert_bit",
		},
		pgx.CopyFromRows(rows),
	)

	return err
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
