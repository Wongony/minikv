package vlog

import (
	"minikv/utils"
	"minikv/utils/codec"
)

type Options struct {
}

// VLog
type VLog struct {
	closer *utils.Closer
}

// Close 关闭资源
func (v *VLog) Close() error {
	return nil
}

// NewVLog
func NewVLog(opt *Options) *VLog {
	v := &VLog{}
	v.closer = utils.NewCloser(1)
	return v
}

// StartGC
func (v *VLog) StartGC() {
	defer v.closer.Done()
	for {
		select {
		case <-v.closer.Wait():
		}
		// gc logic...
	}
}

// Set
func (v *VLog) Set(entry *codec.Entry) error {
	return nil
}

func (v *VLog) Get(entry *codec.Entry) (*codec.Entry, error) {
	// valuePtr := codec.ValuePtrDecode(entry.Value)
	return nil, nil
}