package iotdb_go

import (
	"context"
	"crypto/tls"
	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/pkg/errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type ClientInfo struct {
	Products []struct {
		Name    string
		Version string
	}

	Comment []string
}

// Append returns a new copy of the combined ClientInfo structs
func (a ClientInfo) Append(b ClientInfo) ClientInfo {
	c := ClientInfo{
		Products: make([]struct {
			Name    string
			Version string
		}, 0, len(a.Products)+len(b.Products)),
		Comment: make([]string, 0, len(a.Comment)+len(b.Comment)),
	}

	for _, p := range a.Products {
		c.Products = append(c.Products, p)
	}
	for _, p := range b.Products {
		c.Products = append(c.Products, p)
	}

	for _, cm := range a.Comment {
		c.Comment = append(c.Comment, cm)
	}
	for _, cm := range b.Comment {
		c.Comment = append(c.Comment, cm)
	}

	return c
}

type ConnOpenStrategy uint8

const (
	ConnOpenInOrder ConnOpenStrategy = iota
	ConnOpenRoundRobin
	ConnOpenRandom
)

type Options struct {
	client.PoolConfig
	Debug            bool
	ClientInfo       ClientInfo
	Addr             []string
	ConnOpenStrategy ConnOpenStrategy
	DialContext      func(ctx context.Context, addr string) (net.Conn, error)
	TLS              *tls.Config
	DialTimeout      time.Duration                 // default 30 second
	Debugf           func(format string, v ...any) // only works when Debug is true
}

func ParseDSN(dsn string) (*Options, error) {
	opt := &Options{}
	err := opt.fromDSN(dsn)
	if err != nil {
		return nil, err
	}
	return opt, err
}

func (o *Options) fromDSN(in string) error {
	dsn, err := url.Parse(in)
	if err != nil {
		return err
	}
	if dsn.Host == "" {
		return errors.New("parse dsn address failed")
	}

	if dsn.User != nil {
		o.PoolConfig.UserName = dsn.User.Username()
		o.PoolConfig.Password, _ = dsn.User.Password()
	}

	//o.PoolConfig.Password = strings.TrimPrefix(dsn.Path, "/")
	o.Addr = append(o.Addr, strings.Split(dsn.Host, ",")...)

	var params = dsn.Query()

	for v := range params {
		switch v {
		case "fetch_size":
			{
				fetchSize, err := strconv.ParseInt(params.Get(v), 10, 32)
				if err != nil {
					return errors.Wrap(err, "fetch size invalid value")
				}
				o.PoolConfig.FetchSize = int32(fetchSize)
				break
			}
		case "time_zone":
			{
				o.PoolConfig.TimeZone = params.Get(v)
				break
			}
		case "connect_retry_max":
			{
				connectRetryMax, err := strconv.ParseInt(params.Get(v), 10, 32)
				if err != nil {
					return errors.Wrap(err, "connect retry max invalid value")
				}
				o.PoolConfig.ConnectRetryMax = int(connectRetryMax)
				break
			}
		case "username":
			{
				o.PoolConfig.UserName = params.Get(v)
				break
			}
		case "password":
			{
				o.PoolConfig.Password = params.Get(v)
				break
			}
		default:
			{
				break
			}
		}
	}
	return nil
}
