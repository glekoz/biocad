package service

import (
	"strings"
	"testing"
)

var validHeaders = []string{
	"n", "mqtt", "invid", "unit_guid", "msg_id",
	"text", "context", "class", "level", "area",
	"addr", "block", "type", "bit", "invert_bit",
}

func newTestParser() *Parser {
	return NewParser(validHeaders)
}

// ─── validateHeaders ───────────────────────────────────────────────────────

func TestValidateHeaders_Valid(t *testing.T) {
	p := newTestParser()
	if !p.validateHeaders(validHeaders) {
		t.Fatal("expected valid headers to pass")
	}
}

func TestValidateHeaders_WithWhitespace(t *testing.T) {
	p := newTestParser()
	padded := make([]string, len(validHeaders))
	for i, h := range validHeaders {
		padded[i] = "  " + h + "\t"
	}
	if !p.validateHeaders(padded) {
		t.Fatal("expected headers with surrounding whitespace to pass")
	}
}

func TestValidateHeaders_WrongCount(t *testing.T) {
	p := newTestParser()
	if p.validateHeaders(validHeaders[:5]) {
		t.Fatal("expected too-few headers to fail")
	}
}

func TestValidateHeaders_WrongName(t *testing.T) {
	p := newTestParser()
	bad := append([]string(nil), validHeaders...)
	bad[3] = "wrong_name"
	if p.validateHeaders(bad) {
		t.Fatal("expected wrong header name to fail")
	}
}

func TestValidateHeaders_Empty(t *testing.T) {
	p := newTestParser()
	if p.validateHeaders([]string{}) {
		t.Fatal("expected empty headers to fail")
	}
}

// ─── parse ─────────────────────────────────────────────────────────────────

func validLine() []string {
	return []string{
		"1", "some/topic", "inv-1", "guid-abc", "msg-001",
		"hello world", "ctx1", "classA", "3", "areaX",
		"addrY", "blockZ", "typeT", "0", "1",
	}
}

func TestParse_Valid(t *testing.T) {
	p := newTestParser()
	got, ok := p.parse(validLine())
	if !ok {
		t.Fatalf("expected valid line to parse, errors: %v", got)
	}
	if got[0] != "1" || got[8] != "3" {
		t.Fatalf("unexpected parsed values: %v", got)
	}
}

func TestParse_TrimsWhitespace(t *testing.T) {
	p := newTestParser()
	line := validLine()
	line[2] = "  inv-1  "
	line[4] = "\tmsg-001\t"
	got, ok := p.parse(line)
	if !ok {
		t.Fatalf("expected trimmed line to parse, errors: %v", got)
	}
	if got[2] != "inv-1" || got[4] != "msg-001" {
		t.Fatalf("whitespace not trimmed: %q %q", got[2], got[4])
	}
}

func TestParse_InvalidN(t *testing.T) {
	p := newTestParser()
	line := validLine()
	line[0] = "not-a-number"
	_, ok := p.parse(line)
	if ok {
		t.Fatal("expected parse to fail when n is not an int")
	}
}

func TestParse_InvalidLevel(t *testing.T) {
	p := newTestParser()
	line := validLine()
	line[8] = "high"
	_, ok := p.parse(line)
	if ok {
		t.Fatal("expected parse to fail when level is not an int")
	}
}

func TestParse_WrongFieldCount(t *testing.T) {
	p := newTestParser()
	line := validLine()[:10] // only 10 fields instead of 15
	errs, ok := p.parse(line)
	if ok {
		t.Fatal("expected parse to fail with wrong field count")
	}
	joined := strings.Join(errs, "; ")
	if !strings.Contains(joined, "wrong number of fields") {
		t.Fatalf("unexpected error message: %s", joined)
	}
}

func TestParse_EmptyOptionalFields(t *testing.T) {
	p := newTestParser()
	line := validLine()
	// mqtt, text, context, class, area, addr, block, type, bit, invert_bit are nullable
	line[1] = ""
	line[5] = ""
	got, ok := p.parse(line)
	if !ok {
		t.Fatalf("expected parse with empty optional fields to succeed: %v", got)
	}
}
