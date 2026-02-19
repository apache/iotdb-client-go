package iotdb_go

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
)

type stdConnOpener struct {
	err    error
	opt    *Options
	debugf func(format string, v ...any)
}

func (o *stdConnOpener) Driver() driver.Driver {
	var debugf = func(format string, v ...any) {}
	if o.opt.Debug {
		if o.opt.Debugf != nil {
			debugf = o.opt.Debugf
		} else {
			debugf = log.New(os.Stdout, "[iotdb-std] ", 0).Printf
		}
	}
	return &stdDriver{
		opt:    o.opt,
		debugf: debugf,
	}
}

func (o *stdConnOpener) Connect(ctx context.Context) (_ driver.Conn, err error) {
	if o.err != nil {
		o.debugf("[connect] opener error: %v\n", o.err)
		return nil, o.err
	}
	var (
		conn     stdConnect
		connID   = int(atomic.AddInt64(&globalConnID, 1))
		dialFunc func(ctx context.Context, addr string, num int, opt *Options) (stdConnect, error)
	)

	dialFunc = func(ctx context.Context, addr string, num int, opt *Options) (stdConnect, error) {
		return dial(ctx, addr, num, opt)
	}

	if o.opt.Addr == nil || len(o.opt.Addr) == 0 {
		return nil, ErrAcquireConnNoAddress
	}

	for i := range o.opt.Addr {
		var num int
		switch o.opt.ConnOpenStrategy {
		case ConnOpenInOrder:
			num = i
		case ConnOpenRoundRobin:
			num = (connID + i) % len(o.opt.Addr)
		case ConnOpenRandom:
			random := rand.Int()
			num = (random + i) % len(o.opt.Addr)
		}
		if conn, err = dialFunc(ctx, o.opt.Addr[num], connID, o.opt); err == nil {
			var debugf = func(format string, v ...any) {}
			if o.opt.Debug {
				if o.opt.Debugf != nil {
					debugf = o.opt.Debugf
				} else {
					debugf = log.New(os.Stdout, fmt.Sprintf("[iotdb-std][conn=%d][%s] ", num, o.opt.Addr[num]), 0).Printf
				}
			}
			return &stdDriver{
				conn:   conn,
				debugf: debugf,
			}, nil
		} else {
			o.debugf("[connect] error connecting to %s on connection %d: %v\n", o.opt.Addr[num], connID, err)
		}
	}

	return nil, err
}
