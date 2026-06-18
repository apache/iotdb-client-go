package client

import "context"

type Options struct {
	ctx context.Context
}

// Option 是 Options 的函数式配置项。
type Option func(*Options)

// ApplyOptions 将函数式参数合并为最终配置。
func ApplyOptions(opts ...Option) Options {
	var result Options
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&result)
	}
	return result
}

func WithCtx(ctx context.Context) Option {
	return func(opts *Options) {
		opts.ctx = ctx
	}
}
