-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS records (
    id SERIAL PRIMARY KEY,
    n INTEGER NOT NULL,
    mqtt TEXT,
    invid TEXT NOT NULL,
    unit_guid TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    text TEXT NOT NULL,
    context TEXT,
    class TEXT NOT NULL, -- можно было бы сделать ENUM
    level INTEGER NOT NULL,
    area TEXT NOT NULL,
    addr TEXT NOT NULL,
    block TEXT,
    type TEXT,
    bit TEXT,
    invert_bit TEXT,
    created_at TIMESTAMP DEFAULT NOW() NOT NULL
);
CREATE TABLE IF NOT EXISTS errored_files (
    id SERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    error TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW() NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_unit_guid ON records (unit_guid);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS records;
DROP TABLE IF EXISTS errored_files;
DROP INDEX IF EXISTS idx_records_unit_guid;
-- +goose StatementEnd
