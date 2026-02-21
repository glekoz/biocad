-- name: CountByUnitGUID :one
SELECT count(*) FROM records WHERE unit_guid = $1;

-- name: GetByUnitGUID :many
SELECT id, n, mqtt, invid, unit_guid, msg_id, text, context,
       class, level, area, addr, block, type, bit, invert_bit, created_at
FROM records
WHERE unit_guid = $1
ORDER BY id
LIMIT $2 OFFSET $3;
