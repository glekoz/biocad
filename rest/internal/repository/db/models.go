package db

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type Record struct {
	ID        int32
	N         int32
	Mqtt      pgtype.Text
	Invid     string
	UnitGuid  string
	MsgID     string
	Text      string
	Context   pgtype.Text
	Class     string
	Level     int32
	Area      string
	Addr      string
	Block     pgtype.Text
	Type      pgtype.Text
	Bit       pgtype.Text
	InvertBit pgtype.Text
	CreatedAt time.Time
}
