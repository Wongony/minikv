package minikv

import "minikv/utils"

type Options struct {
	ValueThreshold int64
}

// NewDefaultOptions 返回默认的 options
func NewDefaultOptions() *Options {
	opt := &Options{}
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}
