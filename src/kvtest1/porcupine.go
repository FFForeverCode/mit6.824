package kvtest

import (
	"fmt"
	"io/ioutil"
	//"log"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"6.5840/kvsrv1/rpc"
	"6.5840/models1"
	"6.5840/tester1"
)

const linearizabilityCheckTimeout = 1 * time.Second

type OpLog struct {
	operations []porcupine.Operation
	sync.Mutex
}

func (log *OpLog) Len() int {
	log.Lock()
	defer log.Unlock()
	return len(log.operations)
}

func (log *OpLog) Append(op porcupine.Operation) {
	log.Lock()
	defer log.Unlock()
	log.operations = append(log.operations, op)
}

func (log *OpLog) Read() []porcupine.Operation {
	log.Lock()
	defer log.Unlock()
	ops := make([]porcupine.Operation, len(log.operations))
	copy(ops, log.operations)
	return ops
}

// to make sure timestamps use the monotonic clock, instead of computing
// absolute timestamps with `time.Now().UnixNano()` (which uses the wall
// clock), we measure time relative to `t0` using `time.Since(t0)`, which uses
// the monotonic clock
var t0 = time.Now()

func Get(cfg *tester.Config, ck IKVClerk, key string, log *OpLog, cli int) (string, rpc.Tversion, rpc.Err) {
	start := int64(time.Since(t0))
	val, ver, err := ck.Get(key)
	end := int64(time.Since(t0))
	cfg.Op()
	if log != nil {
		log.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 0, Key: key},
			Output:   models.KvOutput{Value: val, Version: uint64(ver), Err: string(err)},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
	return val, ver, err
}

func Put(cfg *tester.Config, ck IKVClerk, key string, value string, version rpc.Tversion, log *OpLog, cli int) rpc.Err {
	start := int64(time.Since(t0))
	err := ck.Put(key, value, version)
	end := int64(time.Since(t0))
	cfg.Op()
	if log != nil {
		log.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 1, Key: key, Value: value, Version: uint64(version)},
			Output:   models.KvOutput{Err: string(err)},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
	return err
}

// Checks that the log of Clerk.Put's and Clerk.Get's is linearizable (see
// linearizability-faq.txt)
func checkPorcupine(t *testing.T, opLog *OpLog, nsec time.Duration) {
	//log.Printf("oplog len %v %v", ts.oplog.Len(), ts.oplog)
	res, info := porcupine.CheckOperationsVerbose(models.KvModel, opLog.Read(), nsec)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "porcupine-*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}
}

// Porcupine
func (ts *Test) Get(ck IKVClerk, key string, cli int) (string, rpc.Tversion, rpc.Err) {
	start := int64(time.Since(t0))
	val, ver, err := ck.Get(key)
	end := int64(time.Since(t0))
	ts.Op()
	if ts.oplog != nil {
		ts.oplog.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 0, Key: key},
			Output:   models.KvOutput{Value: val, Version: uint64(ver), Err: string(err)},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
	return val, ver, err
}

// Porcupine
func (ts *Test) Put(ck IKVClerk, key string, value string, version rpc.Tversion, cli int) rpc.Err {
	start := int64(time.Since(t0))
	err := ck.Put(key, value, version)
	end := int64(time.Since(t0))
	ts.Op()
	if ts.oplog != nil {
		ts.oplog.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 1, Key: key, Value: value, Version: uint64(version)},
			Output:   models.KvOutput{Err: string(err)},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
	return err
}

func (ts *Test) CheckPorcupine() {
	checkPorcupine(ts.t, ts.oplog, linearizabilityCheckTimeout)
}

func (ts *Test) CheckPorcupineT(nsec time.Duration) {
	checkPorcupine(ts.t, ts.oplog, nsec)
}
