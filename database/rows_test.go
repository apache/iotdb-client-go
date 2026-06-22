package iotdb_go

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubSessionDataSet struct {
	closeErr   error
	closeCount int
}

func (s *stubSessionDataSet) Next() (bool, error) { return false, io.EOF }
func (s *stubSessionDataSet) Close() error {
	s.closeCount++
	return s.closeErr
}

func TestRowsClose_ReturnsDatasetErrorAndReleases(t *testing.T) {
	closeErr := errors.New("close failed")
	closeCount := 0
	releaseCount := 0

	rows := &rows{
		closeFn: func() error {
			closeCount++
			return closeErr
		},
		release: func() { releaseCount++ },
	}

	err := rows.Close()
	require.Error(t, err)
	assert.EqualError(t, err, "close failed")
	assert.Equal(t, 1, closeCount)
	assert.Equal(t, 1, releaseCount)
	assert.Nil(t, rows.release)
}

func TestRowsClose_SuccessReleasesOnce(t *testing.T) {
	dataset := &stubSessionDataSet{}
	releaseCount := 0

	rows := &rows{
		closeFn: dataset.Close,
		release: func() { releaseCount++ },
	}

	err := rows.Close()
	require.NoError(t, err)
	assert.Equal(t, 1, dataset.closeCount)
	assert.Equal(t, 1, releaseCount)
	assert.Nil(t, rows.release)
	assert.Nil(t, rows.set)
	assert.Nil(t, rows.closeFn)
}

func TestRowsClose_IsIdempotent(t *testing.T) {
	closeCount := 0
	releaseCount := 0

	rows := &rows{
		closeFn: func() error {
			closeCount++
			return nil
		},
		release: func() { releaseCount++ },
	}

	err := rows.Close()
	require.NoError(t, err)
	err = rows.Close()
	require.NoError(t, err)
	assert.Equal(t, 1, closeCount)
	assert.Equal(t, 1, releaseCount)
}

func TestRowsNextNilSet(t *testing.T) {
	rows := &rows{}
	ok, err := rows.Next()
	assert.False(t, ok)
	assert.EqualError(t, err, "rows is nil")
}
