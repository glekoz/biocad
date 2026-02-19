package service

import (
	"fmt"
	"strconv"
	"strings"
)

type Parser struct {
	headers []string
	// другие поля для валидации записи, например
	// messageClasses []string
	// или
	// maxLevel int
	// minLevel int
}

func NewParser(hs []string) *Parser {
	return &Parser{
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

// в качестве выходного типа выбрал []string, а не models.TSVRecord,
// так как при создании PDF удобнее работать со строками
func (p *Parser) parse(line []string) ([]string, bool) {
	for i := range line {
		line[i] = strings.TrimSpace(line[i])
	}
	var errFields []string
	if len(line) != len(p.headers) {
		errFields = append(errFields, fmt.Sprintf("wrong number of fields given: %d, expected: %d", len(line), len(p.headers)))
	}

	// n
	_, err := strconv.Atoi(line[0])
	if err != nil {
		errFields = append(errFields, fmt.Sprintf("n is not int, given: %s", line[0]))
	}

	// lvl
	_, err = strconv.Atoi(line[8])
	if err != nil {
		errFields = append(errFields, fmt.Sprintf("lvl is not int, given: %s", line[8]))
	}
	if len(errFields) > 0 {
		return errFields, false
	}
	return line, true
}
