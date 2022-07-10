package utils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestSkipList_compare(t *testing.T) {
	list := SkipList{
		header:   nil,
		rand:     nil,
		maxLevel: 0,
		length:   0,
	}

	byte1 := []byte("1")
	byte2 := []byte("2")
	entry1 := NewEntry(byte1, byte1)

	byte1score := list.calcScore(byte1)
	byte2score := list.calcScore(byte2)

	elem := &Element{
		levels: nil,
		entry:  entry1,
		score:  byte2score,
	}

	assert.Equal(t, list.compare(byte1score, byte1, elem), -1)
}

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList()

	entry1 := NewEntry([]byte("key1"), []byte("val1"))
	assert.Nil(t, list.Add(entry1))
	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)

	entry2 := NewEntry([]byte("key2"), []byte("val2"))
	assert.Nil(t, list.Add(entry2))
	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)

	assert.Nil(t, list.Search([]byte("noexist")))

	entry2_new := NewEntry([]byte("key1"), []byte("val1+1"))
	assert.Nil(t, list.Add(entry2_new))
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList()
	key, val := "", ""
	maxTime := 1000000
	for i := 0; i < maxTime; i++ {
		key, val = fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		res := list.Add(entry)
		assert.Equal(b, res, nil)
		searchVal := list.Search([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))
	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList()
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.Nil(t, l.Add(NewEntry(key(i), key(i))))
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			if v != nil {
				require.EqualValues(t, key(i), v.Value)
				return
			}
			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList()
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.Nil(b, l.Add(NewEntry(key(i), key(i))))
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			if v != nil {
				require.EqualValues(b, key(i), v.Value)
			}
			require.NotNil(b, v)
		}(i)
	}
	wg.Wait()
}
