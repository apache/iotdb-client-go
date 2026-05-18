/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package iotdb_go

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==================== Aggregation Functions ====================

// TestQuery_AggCount verifies COUNT returns the correct row count.
func TestQuery_AggCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.agg_count")
	defer cleanup()

	row := queryConn.QueryRowContext(context.Background(),
		"SELECT COUNT(temp) FROM root.agg_count.d1")
	var count int64
	err := row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

// TestQuery_AggSum verifies SUM aggregation.
func TestQuery_AggSum(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.agg_sum")
	defer cleanup()

	row := queryConn.QueryRowContext(context.Background(),
		"SELECT SUM(temp) FROM root.agg_sum.d1")
	var sum float64
	err := row.Scan(&sum)
	require.NoError(t, err)
	assert.InDelta(t, 110.0, sum, 0.1)
}

// TestQuery_AggAvg verifies AVG aggregation.
func TestQuery_AggAvg(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.agg_avg")
	defer cleanup()

	row := queryConn.QueryRowContext(context.Background(),
		"SELECT AVG(temp) FROM root.agg_avg.d1")
	var avg float64
	err := row.Scan(&avg)
	require.NoError(t, err)
	assert.InDelta(t, 22.0, avg, 0.1)
}

// TestQuery_AggMinMax verifies MIN_VALUE and MAX_VALUE aggregation.
func TestQuery_AggMinMax(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.agg_minmax")
	defer cleanup()

	row := queryConn.QueryRowContext(context.Background(),
		"SELECT MIN_VALUE(temp), MAX_VALUE(temp) FROM root.agg_minmax.d1")
	var minTemp, maxTemp float64
	err := row.Scan(&minTemp, &maxTemp)
	require.NoError(t, err)
	assert.InDelta(t, 20.0, minTemp, 0.1)
	assert.InDelta(t, 24.0, maxTemp, 0.1)
}

// TestQuery_AggFirstLastValue verifies FIRST_VALUE and LAST_VALUE aggregation.
func TestQuery_AggFirstLastValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.agg_firstlast")
	defer cleanup()

	row := queryConn.QueryRowContext(context.Background(),
		"SELECT FIRST_VALUE(temp), LAST_VALUE(temp) FROM root.agg_firstlast.d1")
	var first, last float64
	err := row.Scan(&first, &last)
	require.NoError(t, err)
	assert.InDelta(t, 20.0, first, 0.1)
	assert.InDelta(t, 24.0, last, 0.1)
}

// TestQuery_AggMinMaxTime verifies MIN_TIME and MAX_TIME return results.
func TestQuery_AggMinMaxTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.agg_mintime")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT MIN_TIME(temp), MAX_TIME(temp) FROM root.agg_mintime.d1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	require.NoError(t, rows.Err())
}

// TestQuery_MultipleAggregations verifies multiple aggregation functions in one query.
func TestQuery_MultipleAggregations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.agg_multi")
	defer cleanup()

	row := queryConn.QueryRowContext(context.Background(),
		"SELECT COUNT(temp), SUM(temp), AVG(temp), MIN_VALUE(temp), MAX_VALUE(temp) FROM root.agg_multi.d1")
	var count int64
	var sum, avg, min, max float64
	err := row.Scan(&count, &sum, &avg, &min, &max)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)
	assert.InDelta(t, 110.0, sum, 0.1)
	assert.InDelta(t, 22.0, avg, 0.1)
	assert.InDelta(t, 20.0, min, 0.1)
	assert.InDelta(t, 24.0, max, 0.1)
}

// ==================== GROUP BY ====================

// TestQuery_GroupByTimeInterval verifies time-interval grouping.
func TestQuery_GroupByTimeInterval(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.group_time")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT COUNT(temp), AVG(temp) FROM root.group_time.d1 GROUP BY ([1704067200000, 1704067500000), 2m)")
	require.NoError(t, err)
	defer rows.Close()

	assert.Greater(t, countRows(t, rows), 0)
}

// TestQuery_GroupByWithWhere verifies GROUP BY combined with WHERE.
func TestQuery_GroupByWithWhere(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.group_where")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT COUNT(temp) FROM root.group_where.d1 WHERE time < 1704067440000 GROUP BY ([1704067200000, 1704067500000), 2m)")
	require.NoError(t, err)
	defer rows.Close()

	assert.Greater(t, countRows(t, rows), 0)
}

// ==================== ORDER BY ====================

// TestQuery_OrderByTimeDesc verifies descending time ordering.
func TestQuery_OrderByTimeDesc(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.order_time")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.order_time.d1 ORDER BY time DESC")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 5, countRows(t, rows))
}

// TestQuery_OrderByTimeAsc verifies ascending time ordering.
func TestQuery_OrderByTimeAsc(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.order_asc")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.order_asc.d1 ORDER BY time ASC")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 5, countRows(t, rows))
}

// ==================== LIMIT and OFFSET ====================

// TestQuery_Limit verifies LIMIT restricts row count.
func TestQuery_Limit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.limit_test")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT * FROM root.limit_test.d1 LIMIT 3")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 3, countRows(t, rows))
}

// TestQuery_LimitOffset verifies LIMIT with OFFSET skips rows correctly.
func TestQuery_LimitOffset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.limit_offset")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.limit_offset.d1 ORDER BY time ASC LIMIT 2 OFFSET 2")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 2, countRows(t, rows))
}

// ==================== ALIGN BY ====================

// TestQuery_AlignByDevice verifies cross-device alignment.
func TestQuery_AlignByDevice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.align_device")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp FROM root.align_device.d1, root.align_device.d2 ALIGN BY DEVICE")
	require.NoError(t, err)
	defer rows.Close()

	assert.GreaterOrEqual(t, countRows(t, rows), 1)
}

// TestQuery_AlignByTime verifies time-based alignment.
func TestQuery_AlignByTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.align_time")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT * FROM root.align_time.d1 ALIGN BY TIME")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 5, countRows(t, rows))
}

// ==================== LAST Query ====================

// TestQuery_LastSingleColumn verifies LAST returns one row per column.
func TestQuery_LastSingleColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.last_single")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT LAST temp FROM root.last_single.d1")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 1, countRows(t, rows))
}

// TestQuery_LastMultipleColumns verifies LAST for multiple columns.
func TestQuery_LastMultipleColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.last_multi")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT LAST temp, hum FROM root.last_multi.d1")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 2, countRows(t, rows))
}

// TestQuery_LastAllColumns verifies LAST * returns all columns.
func TestQuery_LastAllColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.last_all")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT LAST * FROM root.last_all.d1")
	require.NoError(t, err)
	defer rows.Close()

	assert.GreaterOrEqual(t, countRows(t, rows), 3)
}

// TestQuery_LastWithWhere verifies LAST with time filter.
func TestQuery_LastWithWhere(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.last_where")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT LAST temp FROM root.last_where.d1 WHERE time >= 1704067320000")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 1, countRows(t, rows))
}

// ==================== Expressions ====================

// TestQuery_ArithmeticExpression verifies arithmetic in SELECT.
func TestQuery_ArithmeticExpression(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.expr_arith")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT temp + 10, temp * 2, temp / 2 FROM root.expr_arith.d1")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 5, countRows(t, rows))
}

// TestQuery_AggregationExpression verifies arithmetic on aggregation results.
func TestQuery_AggregationExpression(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.expr_agg")
	defer cleanup()

	row := queryConn.QueryRowContext(context.Background(),
		"SELECT AVG(temp) + 1, SUM(temp) * 2 FROM root.expr_agg.d1")
	var avgPlus, sumTimes float64
	err := row.Scan(&avgPlus, &sumTimes)
	require.NoError(t, err)
	assert.InDelta(t, 23.0, avgPlus, 0.1)
	assert.InDelta(t, 220.0, sumTimes, 0.1)
}

// ==================== Metadata ====================

// TestQuery_ShowDatabases verifies SHOW DATABASES returns results.
func TestQuery_ShowDatabases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	rows, err := queryConn.QueryContext(context.Background(), "SHOW DATABASES")
	require.NoError(t, err)
	defer rows.Close()

	assert.GreaterOrEqual(t, countRows(t, rows), 1)
}

// TestQuery_ShowTimeseries verifies SHOW TIMESERIES returns correct count.
func TestQuery_ShowTimeseries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.show_ts")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SHOW TIMESERIES root.show_ts.**")
	require.NoError(t, err)
	defer rows.Close()

	assert.Equal(t, 4, countRows(t, rows))
}

// TestQuery_ColumnsMetadata verifies column names are retrievable from result set.
func TestQuery_ColumnsMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cleanup := setupQueryTestData(t, queryConn, "root.col_meta")
	defer cleanup()

	rows, err := queryConn.QueryContext(context.Background(),
		"SELECT * FROM root.col_meta.d1")
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(cols), 3)
}
