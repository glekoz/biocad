package service

import "errors"

var (
	ErrNotFound      = errors.New("records not found")
	ErrInvalidPage   = errors.New("page must be >= 1")
	ErrInvalidLimit  = errors.New("limit must be between 1 and 100")
	ErrEmptyUnitGUID = errors.New("unit_guid is required")
)
