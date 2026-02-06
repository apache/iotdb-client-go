package iotdb_go

import (
	"context"
	"maps"
	"slices"
	"time"
)

type Parameters map[string]string

type Settings map[string]any

// ColumnNameAndType represents a column name and type
type ColumnNameAndType struct {
	Name string
	Type string
}

var _contextOptionKey = &QueryOptions{
	settings: Settings{
		"_contextOption": struct{}{},
	},
}

type (
	QueryOption  func(*QueryOptions) error
	AsyncOptions struct {
		ok   bool
		wait bool
	}
	QueryOptions struct {
		async    AsyncOptions
		queryID  string
		quotaKey string
		jwt      string

		settings            Settings
		parameters          Parameters
		blockBufferSize     uint8
		userLocation        *time.Location
		columnNamesAndTypes []ColumnNameAndType
		clientInfo          ClientInfo
	}
)

// clone returns a copy of QueryOptions where Settings and Parameters are safely mutable.
func (q *QueryOptions) clone() QueryOptions {
	c := QueryOptions{
		async:               q.async,
		queryID:             q.queryID,
		quotaKey:            q.quotaKey,
		settings:            nil,
		parameters:          nil,
		blockBufferSize:     q.blockBufferSize,
		userLocation:        q.userLocation,
		columnNamesAndTypes: nil,
	}

	if q.settings != nil {
		c.settings = maps.Clone(q.settings)
	}

	if q.parameters != nil {
		c.parameters = maps.Clone(q.parameters)
	}

	if q.columnNamesAndTypes != nil {
		c.columnNamesAndTypes = slices.Clone(q.columnNamesAndTypes)
	}

	if q.clientInfo.Products != nil || q.clientInfo.Comment != nil {
		c.clientInfo = q.clientInfo.Append(ClientInfo{})
	}

	return c
}

// queryOptions returns a mutable copy of the QueryOptions struct within the given context.
// If iotdb context was not provided, an empty struct with a valid Settings map is returned.
// If the context has a deadline greater than 1s then max_execution_time setting is appended.
func queryOptions(ctx context.Context) QueryOptions {
	var opt QueryOptions

	if ctxOpt, ok := ctx.Value(_contextOptionKey).(QueryOptions); ok {
		opt = ctxOpt.clone()
	} else {
		opt = QueryOptions{
			settings: make(Settings),
		}
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return opt
	}

	if sec := time.Until(deadline).Seconds(); sec > 1 {
		opt.settings["max_execution_time"] = int(sec + 5)
	}

	return opt
}
