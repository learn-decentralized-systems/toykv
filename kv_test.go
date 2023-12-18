package toykv

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"os"
	"testing"
	"time"
)
import "github.com/stretchr/testify/assert"

func TestKeyValue_Set(t *testing.T) {
	_ = os.RemoveAll("store.db")
	kv := KeyValueStore{}
	err := kv.Open("store.db")
	assert.Nil(t, err)
	assert.NotNil(t, kv.DB)

	now := time.Now()
	err = kv.Merge('T', now.String(), "now")
	assert.Nil(t, err)
	_, err = kv.Get('T', now.String())
	assert.Equal(t, pebble.ErrNotFound, err)
	err = kv.Commit()
	assert.Nil(t, err)
	str, err := kv.Get('T', now.String())
	assert.Nil(t, err)
	assert.Equal(t, "now", str)

	err = kv.Merge('T', now.String(), " is ")
	assert.Nil(t, err)
	err = kv.Merge('T', now.String(), now.String())
	assert.Nil(t, err)
	err = kv.Commit()
	assert.Nil(t, err)
	str, err = kv.Get('T', now.String())
	assert.Nil(t, err)
	assert.Equal(t, "now is "+now.String(), str)

	kv.Close()
	assert.Nil(t, kv.DB)
	_ = os.RemoveAll("store.db")
}

type Int struct {
	i int
}

func (i Int) String() string {
	return fmt.Sprintf("%8d", i.i)
}

func (i *Int) Inc() {
	i.i++
}

func TestKeyValue_Range(t *testing.T) {
	_ = os.RemoveAll("range.store")
	kv := KeyValueStore{}
	err := kv.Open("range.store")
	assert.Nil(t, err)
	assert.NotNil(t, kv.DB)
	i := Int{0}

	for i.i < 1<<20 {
		err = kv.Set('N', i.String(), "set")
		assert.Nil(t, err)
		i.Inc()
	}
	err = kv.Commit()
	assert.Nil(t, err)

	fro := Int{1100}
	to := Int{2233}

	i = fro
	rng := kv.Range('N', fro.String(), to.String())
	for rng.Valid() {
		assert.Equal(t, uint8('N'), rng.Liter())
		assert.Equal(t, i.String(), rng.Key())
		assert.Equal(t, "set", rng.Value())
		i.Inc()
		rng.Next()
	}
	assert.Equal(t, to.i, i.i)
	assert.Equal(t, uint8(0), rng.Liter())
	assert.Equal(t, "", rng.Value())
	assert.Equal(t, "", rng.Key())
	assert.False(t, rng.Next())

	kv.Close()
	assert.Nil(t, kv.DB)
	_ = os.RemoveAll("range.store")
}
