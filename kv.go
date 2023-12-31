package toykv

import (
	"bytes"
	"errors"
	"github.com/cockroachdb/pebble"
	"strings"
)

type KeyValueStore struct {
	DB    *pebble.DB
	batch pebble.Batch
	sync  bool
}

var ErrAlreadyOpen = errors.New("the database is already open")

func (kv *KeyValueStore) Open(name string) (err error) {
	if kv.DB != nil {
		return ErrAlreadyOpen
	}
	kv.sync = strings.HasSuffix(name, ".db")
	o := pebble.Options{
		DisableWAL: !kv.sync,
	}
	kv.DB, err = pebble.Open(name, &o)
	return
}

func (kv *KeyValueStore) Set(lit byte, key string, value string) error {
	k := composeKey(lit, key)
	wo := pebble.WriteOptions{Sync: kv.sync}
	return kv.batch.Set(k, []byte(value), &wo)
}

func (kv *KeyValueStore) Merge(lit byte, key string, value string) error {
	k := composeKey(lit, key)
	wo := pebble.WriteOptions{Sync: kv.sync}
	return kv.batch.Merge(k, []byte(value), &wo)
}

func (kv *KeyValueStore) Commit() (err error) {
	wo := pebble.WriteOptions{Sync: kv.sync}
	err = kv.DB.Apply(&kv.batch, &wo)
	if err == nil {
		kv.batch = pebble.Batch{}
	}
	return
}

func (kv *KeyValueStore) Get(lit byte, key string) (value string, err error) {
	k := composeKey(lit, key)
	val, closr, err := kv.DB.Get(k)
	if err != nil {
		return
	}
	value = string(val)
	_ = closr.Close()
	return
}

type KeyValueIterator struct {
	iter *pebble.Iterator
}

func composeKey(lit byte, str string) []byte {
	ret := make([]byte, 0, len(str)+1)
	ret = append(ret, lit)
	ret = append(ret, str...)
	return ret
}

func (kv *KeyValueStore) Range(lit byte, from, till string) (kvi KeyValueIterator) {
	fro := composeKey(lit, from)
	to := composeKey(lit, till)
	if bytes.Compare(fro, to) > 0 {
		fro, to = to, fro
	}
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: to,
	}
	kvi.iter = kv.DB.NewIter(&io)
	if !kvi.iter.SeekGE(fro) {
		kvi.Close()
	}
	return
}

func (i *KeyValueIterator) Valid() bool {
	return i.iter != nil && i.iter.Valid()
}

func (i *KeyValueIterator) Liter() byte {
	if i.iter == nil {
		return 0
	} else {
		return i.iter.Key()[0]
	}
}

func (i *KeyValueIterator) Key() string {
	if i.iter == nil {
		return ""
	} else {
		return string(i.iter.Key()[1:])
	}
}

func (i *KeyValueIterator) Value() string {
	if i.iter == nil {
		return ""
	}
	v, e := i.iter.ValueAndErr()
	if e == nil {
		return string(v)
	} else {
		_ = i.iter.Close()
		i.iter = nil
		return ""
	}
}

func (i *KeyValueIterator) Next() bool {
	if i.iter == nil {
		return false
	}
	ret := i.iter.Next()
	if !ret {
		i.Close()
	}
	return ret
}

func (i *KeyValueIterator) Close() {
	if i.iter != nil {
		_ = i.iter.Close()
		i.iter = nil
	}
}

func (kv *KeyValueStore) Close() {
	if kv.DB != nil {
		_ = kv.DB.Close()
		kv.DB = nil
	}
}
