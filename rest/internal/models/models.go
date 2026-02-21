package models

import "time"

type Record struct {
	ID        int
	N         int
	Mqtt      *string
	Invid     string
	UnitGuid  string
	MsgID     string
	Text      string
	Context   *string
	Class     string
	Level     int
	Area      string
	Addr      string
	Block     *string
	Type      *string
	Bit       *string
	InvertBit *string
	CreatedAt time.Time
}

type PaginatedRecords struct {
	Records []Record
	Page    int
	Limit   int
	Total   int
}
