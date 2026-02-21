package db

import "context"

const countByUnitGUID = `-- name: CountByUnitGUID :one
SELECT count(*) FROM records WHERE unit_guid = $1
`

func (q *Queries) CountByUnitGUID(ctx context.Context, unitGuid string) (int64, error) {
	var count int64
	err := q.db.QueryRow(ctx, countByUnitGUID, unitGuid).Scan(&count)
	return count, err
}

const getByUnitGUID = `-- name: GetByUnitGUID :many
SELECT id, n, mqtt, invid, unit_guid, msg_id, text, context,
       class, level, area, addr, block, type, bit, invert_bit, created_at
FROM records
WHERE unit_guid = $1
ORDER BY id
LIMIT $2 OFFSET $3
`

type GetByUnitGUIDParams struct {
	UnitGuid string
	Limit    int32
	Offset   int32
}

func (q *Queries) GetByUnitGUID(ctx context.Context, arg GetByUnitGUIDParams) ([]Record, error) {
	rows, err := q.db.Query(ctx, getByUnitGUID, arg.UnitGuid, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(
			&r.ID, &r.N, &r.Mqtt, &r.Invid, &r.UnitGuid,
			&r.MsgID, &r.Text, &r.Context, &r.Class, &r.Level,
			&r.Area, &r.Addr, &r.Block, &r.Type, &r.Bit,
			&r.InvertBit, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

const countErroredFiles = `-- name: CountErroredFiles :one
SELECT count(*) FROM errored_files
`

func (q *Queries) CountErroredFiles(ctx context.Context) (int64, error) {
	var count int64
	err := q.db.QueryRow(ctx, countErroredFiles).Scan(&count)
	return count, err
}

const getErroredFiles = `-- name: GetErroredFiles :many
SELECT id, filename, error, created_at
FROM errored_files
ORDER BY created_at DESC
LIMIT $1 OFFSET $2
`

type GetErroredFilesParams struct {
	Limit  int32
	Offset int32
}

func (q *Queries) GetErroredFiles(ctx context.Context, arg GetErroredFilesParams) ([]ErroredFile, error) {
	rows, err := q.db.Query(ctx, getErroredFiles, arg.Limit, arg.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []ErroredFile
	for rows.Next() {
		var f ErroredFile
		if err := rows.Scan(&f.ID, &f.Filename, &f.Error, &f.CreatedAt); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}
