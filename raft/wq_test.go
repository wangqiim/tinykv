package raft

import (
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/stretchr/testify/assert"
)

func TestSimpleStorage(t *testing.T) {
	s := NewMemoryStorage()
	// storage |{}--------
	assert.Equal(t, uint64(1), s.firstIndex())
	assert.Equal(t, uint64(0), s.lastIndex())

	assert.Nil(t, nil, s.Append(nil))

	assert.Equal(t, uint64(1), s.firstIndex())
	assert.Equal(t, uint64(0), s.lastIndex())

	// storage |{}--------
	// append  |---{1}-{2}-
	assert.Nil(t, s.Append([]pb.Entry{{Index: 1}, {Index: 2}}))

	// storage |{}-{1}-{2}-
	assert.Equal(t, uint64(1), s.firstIndex())
	assert.Equal(t, uint64(2), s.lastIndex())

	// overlap
	// storage |{}-{1}-{2}-
	// append  |---{1}-{2}-{3}-{4}-
	assert.Nil(t, s.Append([]pb.Entry{{Index: 1}, {Index: 2}, {Index: 3}, {Index: 4}}))

	// storage |{}-{1}-{2}-{3}-{4}-
	assert.Equal(t, uint64(1), s.firstIndex())
	assert.Equal(t, uint64(4), s.lastIndex())

	// empty hole
	// storage |{}-{1}-{2}-{3}-{4}-
	// append  |--------------------{6}-{7}--
	assert.Panics(t, func() {
		s.Append([]pb.Entry{{Index: 6}, {Index: 7}})
	})

	// storage |{}-{1}-{2}-{3}-{4}-
	assert.Equal(t, uint64(1), s.firstIndex())
	assert.Equal(t, uint64(4), s.lastIndex())
}

func TestSimplelog(t *testing.T) {
	s := NewMemoryStorage()
	raftLog := newLog(s)
	lastIndex := raftLog.append(pb.Entry{Index: 1}, pb.Entry{Index: 2})
	assert.Equal(t, uint64(2), lastIndex)

}

func TestSimple(t *testing.T) {
	a := []int{1, 2, 3}
	b := make([]*int, 3)
	b[0] = &a[0]
	*b[0] = 3
	assert.Equal(t, a[0], 3)
}
