package iotdb_go

import (
	"context"
)

func (c *connect) exec(ctx context.Context, query string, args ...any) error {
	var (
		options   = queryOptions(ctx)
		body, err = bindQueryOrAppendParameters(&options, query, c.timeZone, args...)
	)
	if err != nil {
		return err
	}

	session, err := c.conn.GetSession()
	if err != nil {
		return err
	}
	defer c.conn.PutBack(session)

	_, err = session.ExecuteStatement(body)
	if err != nil {
		return err
	}
	return nil
}
