package minikv

import (
	"minikv/iterator"
	"minikv/lsm"
	"minikv/utils"
	"minikv/utils/codec"
	"minikv/vlog"
)

type (
	// minikv 对外提供的功能集合
	MiniAPI interface {
		Set(data *codec.Entry) error
		Get(key []byte) (*codec.Entry, error)
		Del(key []byte) error
		NewIterator(opt *iterator.Options) iterator.Iterator
		Info() *Stats
		Close() error
	}

	// DB 对外暴露的接口对象 全局唯一,持有各种资源句柄
	DB struct {
		opt   *Options
		lsm   *lsm.LSM
		vlog  *vlog.VLog
		stats *Stats
	}
)

func Open(options *Options) *DB {
	db := &DB{opt: options}
	// 初始化 LSM 结构
	db.lsm = lsm.NewLSM(&lsm.Options{})
	// 初始化 vlog 结构
	db.vlog = vlog.NewVLog(&vlog.Options{})
	// 初始化统计信息
	db.stats = newStats(options)
	// 启动 sstable 的合并压缩过程
	go db.lsm.StartMerge()
	// 启动 vlog gc 过程
	go db.vlog.StartGC()
	// 启动 info 统计过程
	go db.stats.StartStats()
	return db
}

func (db *DB) Close() error {
	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.Close(); err != nil {
		return err
	}
	if err := db.stats.close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Del(key []byte) error {
	// 写入一个值为 nil 的 entry 作为标记实现删除
	return db.Set(&codec.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
}

func (db *DB) Set(data *codec.Entry) error {
	// 做一些必要性的检查
	// 如果 value 大于一个阙值则创建值指针，并将其写入 vlog 中
	var valuePtr *codec.ValuePtr
	if utils.ValueSize(data.Value) > db.opt.ValueThreshold {
		valuePtr = codec.NewValuePtr(data)
		// 先写入 vlog 不会有事务问题，因为如果 lsm 写入失败，vlog 会在 GC 阶段清理无效的 key
		if err := db.vlog.Set(data); err != nil {
			return err
		}
	}
	// 写入 LSM，如果写值指针不空则替换值 entry.value 的值
	if valuePtr != nil {
		data.Value = codec.ValuePtrCodec(valuePtr)
	}
	return db.lsm.Set(data)
}

func (db *DB) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	// 检查输入
	// 从内存表中读取数据
	if entry, err = db.lsm.Get(key); err == nil {
		return entry, err
	}
	// 检查从 lsm 拿到的 value 是否是 value ptr，是则从 vlog 中拿值
	if entry != nil && codec.IsValuePtr(entry) {
		if entry, err = db.vlog.Get(entry); err == nil {
			return entry, err
		}
	}
	return nil, nil
}

func (db *DB) Info() *Stats {
	// 读取 stats 结构，打包数据并返回
	return db.stats
}
