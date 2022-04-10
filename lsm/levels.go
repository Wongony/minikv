package lsm

import (
	"minikv/file"
	"minikv/utils"
	"minikv/utils/codec"
)

type levelManager struct {
	opt			*Options
	cache		*cache
	manifest	*file.Manifest
	levels		[]*levelHandler
}

type levelHandler struct {
	levelNum	int
	tables		[]*table
}

func (lh *levelHandler) close() error {
	return nil
}

func (lh *levelHandler) Get(key []byte) (*codec.Entry, error) {
	// 如果是第 0 层文件则进行特殊处理
	if lh.levelNum == 0 {
		// logic
	} else {
		// logic
	}
	return nil, nil
}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifest.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{}
	lm.opt = opt
	// 读取 manifest 文件构建管理器
	lm.loadManifest()
	lm.build()
	return lm
}

func (lm *levelManager) loadCache() {
}

func (lm *levelManager) loadManifest() {
	lm.manifest = file.OpenManifest(&file.Options{})
}

func (lm *levelManager) build() {
	// 如果 manifest 文件是空的 进行初始化
	lm.levels = make([]*levelHandler, 8)
	lm.levels[0] = &levelHandler{tables: []*table{openTable(lm.opt)}, levelNum: 0}
	for num := 1; num < utils.MaxLevelNum; num++ {
		lm.levels[num] = &levelHandler{tables: []*table{openTable(lm.opt)}, levelNum: num}
	}
	// 逐一加载 sstable 的 index block 构建 cache
	lm.loadCache()
}

func (lm *levelManager) flush(immutable *memTable) error {
	// 向 L0 层 flush 一个 sstable
	return nil
}

func (lm *levelManager) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err error
	)
	// L0 层查询
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// L1-7 层查询
	for level := 1; level < 8; level++ {
		ld := lm.levels[level]
		if entry, err = ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, nil
}