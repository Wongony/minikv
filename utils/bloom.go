package utils

import (
	"math"
)

type Filter []byte

func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

/*
 * 判断一个数据是否在 bloom 过滤器中
 * k 个 Hash 函数计算,判断对应位置是否全被标记为 1
 */
func (f Filter) MayContain(h uint32) bool {
	if len(f) < 2 {
		return false
	}
	k := f[len(f)-1]
	if k > 30 {
		return true
	}
	nBits := uint32(8 * (len(f) - 1))
	delta := (h >> 17) | (h << 15)
	for i := uint8(0); i < k; i++ {
		bitPos := h % nBits
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

/*
 * 阅读 bloom 论文实现,并在这里编写公式
 * 传入参数 numEntries 是 bloom 中存储的数据个数, fp 是 false positive 假阳性率
 */
func BloomBitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(float64(0.69), 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

/*
 * 将多个 key 值放入到 bloom 过滤器中
 */
func appendFilter(keys []uint32, bitsPerKey int) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	nBits := len(keys) * int(bitsPerKey)
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	filter := make([]byte, nBytes+1)
	for _, h := range keys {
		delta := (h >> 17) | (h << 15)
		for i := uint32(0); i < k; i++ {
			bitPos := h % uint32(nBits)
			filter[bitPos/8] |= (1 << (bitPos % 8))
			h += delta
		}
	}
	filter[nBytes] = uint8(k)

	return filter
}

func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += (uint32(b[0])) | (uint32(b[1]) << 8) | (uint32(b[2]) << 16) | (uint32(b[3]) << 24)
		h *= m
		h ^= (h >> 16)
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= (h >> 24)
	}
	return h
}
