package lsm

import (
	"minikv/utils"
	"minikv/utils/codec"
)

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
}

// Options
type Options struct {
}

// 关闭 lsm
func (lsm *LSM) Close() error {
	if err := lsm.memTable.close(); err != nil {
		return err
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	// 等待合并过程的结束
	lsm.closer.Close()
	return nil
}

// NewLSM
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	// 启动 DB 恢复过程加载 wal，如果没有恢复内容则创建新的内存表
	lsm.memTable, lsm.immutables = recovery(opt)
	// 初始化 levelManager
	lsm.levels = newLevelManager(opt)
	// 初始化 closer 用于资源回收的信号控制
	lsm.closer = utils.NewCloser(1)
	return lsm
}

// StartMerge
func (lsm *LSM) StartMerge() {
	defer lsm.closer.Done()
	for {
		select {
		case <-lsm.closer.Wait():
		}
		// 处理并发的合并过程
	}
}

func (lsm *LSM) Set(entry *codec.Entry) error {
	// 检查当前 memtable 是否写满，是的话创建新的 memtable，并将当前内存表写到 immutables 中
	// 否则写入当前 memetable 中
	if err := lsm.memTable.set(entry); err != nil {
		return err
	}
	// 检查是否存在 immutable 需要刷盘
	for _, immutable := range lsm.immutables {
		if err := lsm.levels.flush(immutable); err != nil {
			return nil
		}
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	// 从内存表中查询，先查活跃表，在查不变表
	if entry, err = lsm.memTable.Get(key); entry != nil {
		return entry, err
	}
	for _, imm := range lsm.immutables {
		if entry, err = imm.Get(key); entry != nil {
			return entry, err
		}
	}
	// 从 level manager 查询
	return lsm.levels.Get(key)
}
