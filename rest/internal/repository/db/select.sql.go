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
