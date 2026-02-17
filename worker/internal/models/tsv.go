package models

type TSVRecord struct {
	N         int
	MQTT      string
	InvID     string
	UnitGUID  string
	MsgID     string
	Text      string
	Context   string
	Class     string
	Level     int
	Area      string
	Addr      string
	Block     string
	Type      string
	Bit       string
	InvertBit string
}
