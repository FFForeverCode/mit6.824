package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	l string
	version int
	chosen map[string]bool
	uuid string 
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.ck.Put(l, "", 0)
	lk.chosen = make(map[string]bool)
	lk.l = l
	lk.uuid = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here

	for  {
		acq, ver, _ := lk.ck.Get(lk.l)
		lk.version = int(ver)

		if acq == "" {
			break
		}
		time.Sleep(50 * time.Millisecond)

	}
	lk.ck.Put(lk.l, lk.uuid, rpc.Tversion(lk.version))

	v, _, _ := lk.ck.Get(lk.l)
	if v != lk.uuid {
		lk.Acquire()
	} 
	
}

func (lk *Lock) Release() {
	// Your code here
	lk.ck.Put(lk.l, "", rpc.Tversion(lk.version) + 1)

}
