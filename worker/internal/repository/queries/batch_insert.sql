-- name: InsertRecord :exec
INSERT INTO records (
    n,
    mqtt,
    invid,
    unit_guid,
    msg_id,
    text,
    context,
    class,
    level,
    area,
    addr,
    block,
    type,
    bit,
    invert_bit
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15
);

-- name: InsertErroredFile :exec
INSERT INTO errored_files (
    filename,
    error
) VALUES (
    $1,
    $2
);
