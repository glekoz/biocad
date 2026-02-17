package service

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/glekoz/biocad/worker/internal/models"
)

type Parser struct {
	messageClasses []string
	headers        []string
}

func NewParser(hs []string) *Parser {
	return &Parser{
		// messageClasses: []string{"alarm", "warning", "info", "event", "command", "working"},
		headers: hs,
	}
}

func (p *Parser) validateHeaders(hs []string) bool {
	if len(hs) != len(p.headers) {
		return false
	}
	for i, h := range hs {
		if strings.TrimSpace(h) != p.headers[i] {
			return false
		}
	}
	return true
}

func (p *Parser) newRecord(line []string) (models.TSVRecord, bool) {
	if len(line) != len(p.headers) {
		return models.TSVRecord{}, false
	}
	n, err := strconv.Atoi(line[0])
	if err != nil {
		return models.TSVRecord{}, false
	}

	ok := false
	for i := range p.messageClasses {
		if p.messageClasses[i] == line[7] {
			ok = true
			break
		}
	}
	if !ok {
		return models.TSVRecord{}, false
	}
	lvl, err := strconv.Atoi(line[8])
	if err != nil {
		return models.TSVRecord{}, false
	}
	return models.TSVRecord{
		N:         n,
		MQTT:      line[1],
		InvID:     line[2],
		UnitGUID:  line[3],
		MsgID:     line[4],
		Text:      line[5],
		Context:   line[6],
		Class:     line[7],
		Level:     lvl,
		Area:      line[9],
		Addr:      line[10],
		Block:     line[11],
		Type:      line[12],
		Bit:       line[13],
		InvertBit: line[14],
	}, true
}

func (p *Parser) newStringRecord(line []string) ([]string, bool) {
	for i := range line {
		line[i] = strings.TrimSpace(line[i])
	}
	var errFields []string
	if len(line) != len(p.headers) {
		errFields = append(errFields, "len of parsed line")
		// return nil, false
	}
	// n
	_, err := strconv.Atoi(line[0])
	if err != nil {
		errFields = append(errFields, fmt.Sprintf("n given: %s", line[0]))
		// return nil, false
	}

	// ok := slices.Contains(p.messageClasses, line[7])
	// if !ok {
	// 	errFields = append(errFields, fmt.Sprintf("class given: %s", line[7]))
	// 	// return nil, false
	// }
	// lvl
	_, err = strconv.Atoi(line[8])
	if err != nil {
		errFields = append(errFields, fmt.Sprintf("lvl given: %s", line[8]))
		// return nil, false
	}
	if len(errFields) > 0 {
		return errFields, false
	}
	return line, true
}
