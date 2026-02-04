package driver

import "time"

type (
	NamedValue struct {
		Name  string
		Value any
	}

	NamedDateValue struct {
		Name  string
		Value time.Time
		Scale uint8
	}
)
