package lsm

import (
	"minikv/file"
	"minikv/utils"
	"minikv/utils/codec"
)

// MemTable
type memTable struct {
	wal *file.WalFile
	sl  *utils.Skiplist
}

// Close
func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	if err := m.sl.Close(); err != nil {
		return err
	}
	return nil
}

func (m *memTable) set(entry *codec.Entry) error {
	// 写到 wal 日志，防止崩溃
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 写到 memtable 中
	if err := m.sl.Add(entry); err != nil {
		return err
	}
	return nil
}

func (m *memTable) Get(key []byte) (*codec.Entry, error) {
	// 索引检查当前的 key 是否在表中 O(1) 的时间复杂度
	// 从内存表中获取数据
	return m.sl.Search(key), nil
}

// recovery
func recovery(opt *Options) (*memTable, []*memTable) {
	fileOpt := &file.Options{}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkipList()}, []*memTable{}
}