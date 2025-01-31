package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	kvtest.IKVClerk
	l   string
	id  string
	ver rpc.Tversion
}

func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{IKVClerk: ck}
	// You may add core here
	return lk
}

func (lk *Lock) AcquireLeadership() {
	for {
		if val, ver, err := lk.Get(lk.l); err == rpc.OK {
			if val == "" { // put only when lock is free
				if err := lk.Put(lk.l, lk.id, ver); err == rpc.OK {
					lk.ver = ver + 1
					return
				} else if err == rpc.ErrMaybe { // check if put succeeded?
					if val, ver, err := lk.Get(lk.l); err == rpc.OK {
						if val == lk.id {
							lk.ver = ver
							return
						}
					}
				}
			}
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// for two testing purposes: 1) for the ctrler that is a leader to
// give up its leadership; 2) to take back leadership from a
// partitioned/deposed ctrler using a new ctrler.
func (lk *Lock) ReleaseLeadership() rpc.Err {
	_, ver, err := lk.Get(lk.l)
	if err != rpc.OK {
		log.Printf("ResetLock: %v err %v", lk.l, err)
	}
	if err := lk.Put(lk.l, "", ver); err == rpc.OK || err == rpc.ErrMaybe {
		return rpc.OK
	} else {
		return err
	}
}
