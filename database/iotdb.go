package iotdb_go

import (
	"context"
	"errors"
)

// nativeTransport represents an implementation (TCP or HTTP) that can be pooled by the main iotdb struct.
// Implementations are not expected to be thread safe, which is why we provide acquire/release functions.
type nativeTransport interface {
	query(ctx context.Context, release nativeTransportRelease, query string, args ...any) (*rows, error)
}

// nativeTransport represents an implementation (TCP or HTTP) that can be pooled by the main iotdb struct.
// Implementations are not expected to be thread safe, which is why we provide acquire/release functions.
type nativeTransportAcquire func(context.Context) (nativeTransport, error)
type nativeTransportRelease func(nativeTransport, error)

var (
	ErrBatchInvalid              = errors.New("iotdb: batch is invalid. check appended data is correct")
	ErrBatchAlreadySent          = errors.New("iotdb: batch has already been sent")
	ErrBatchNotSent              = errors.New("iotdb: invalid retry, batch not sent yet")
	ErrAcquireConnTimeout        = errors.New("iotdb: acquire conn timeout. you can increase the number of max open conn or the dial timeout")
	ErrUnsupportedServerRevision = errors.New("iotdb: unsupported server revision")
	ErrBindMixedParamsFormats    = errors.New("iotdb [bind]: mixed named, numeric or positional parameters")
	ErrAcquireConnNoAddress      = errors.New("iotdb: no valid address supplied")
	ErrServerUnexpectedData      = errors.New("code: 101, message: Unexpected packet Data received from client")
	ErrConnectionClosed          = errors.New("iotdb: connection is closed")
)
