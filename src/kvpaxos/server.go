package kvpaxos

import (
	"encoding/gob"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	last_rpcs map[string]int
	data   map[string]string
	opChan chan Op
	curSeq int
}

func (kv *KVPaxos) handleOp() {
	DPrintf("[Server %d loop] start\n", kv.me)
	for !kv.isdead() {
		select {
		case op := <-kv.opChan:
			DPrintf("[Server %d handleOp] handle a op [%s]\n", kv.me, op.toString())
			kv.sync(op)
			DPrintf("[Server %d handleOp] finished op [%s]\n", kv.me, op.toString())
		}
	}
}

func (kv *KVPaxos) waitPaxosDecided(seq int) interface{} {
	DPrintf("[Server %d waitPaxosDecided] start with %d\n", kv.me, seq)
	to := 10 * time.Millisecond
	for {
		fate, v := kv.px.Status(seq)
		DPrintf("[Server %d waitPaxosDecided] Seq %d get fate=%v\n", kv.me, seq, fate)
		if fate == paxos.Decided {
			switch _v := v.(type) {
			case Op:
				DPrintf("[Server %d waitPaxosDecided] seq %d finished. value=%s\n",
					kv.me, seq, _v.toString())
				return v
			default:
				ERROR("impossible\n")
			}
		}else {
			DPrintf("[Server %d waitPaxosDecided] Seq %d slepp to wait Decided\n",
			kv.me, seq)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

/**
 * do the op

 * @op the operation to do
 * @return the value if the op is GetOp, otherwise nil
 */
func (kv *KVPaxos) doOp(op Op) interface{} {
	DPrintf("[Server %d doOp] Start op=%s\n", kv.me, op.toString())
	switch e := op.(type) {
	case *GetOp:
		if v, ok := kv.data[e.Key]; ok {
			DPrintf("[Server %d doOp] GetOp v=%s\n", kv.me, v)
			return v
		} else {
			DPrintf("[Server %d doOp] GetOp v= ErrNoKey\n", kv.me)
			return ""
		}
	case *PutOp:
		kv.data[e.Key] = e.Value
		DPrintf("[Server %d doOp] database update %s\n", kv.me,
			kv.data[e.Key])
		return nil
	case *PutAppendOp:
		_, ok := kv.data[e.Key]
		if ok {
			kv.data[e.Key] += e.Value
		} else {
			kv.data[e.Key] = e.Value
		}
		DPrintf("[Server %d doOp] database update %s\n", kv.me,
			kv.data[e.Key])
		return nil
	}
	DPrintf("[Server %d doOp] finished\n", kv.me)
	return nil
}

/**
 * sync the data to the lasted version
 * 1. doPaxos to determine the op in current seq
 * 2. doOp
 * if the op reach the lasted op, set the ResChan, otherwise loop to sync
 *
 * @op the lasted op want to do
 *
 */
func (kv *KVPaxos) sync(op Op) {
	DPrintf("[Server %d sync] Start with op=%s\n", kv.me, op.toString())
	for !kv.isdead() {
		v := kv.doPaxos(op)
		res := kv.doOp(v)
		if v.getToken() == op.getToken() {
			DPrintf("[Server %d sync] reach the last op\n", kv.me)
			DPrintf("[Server %d sync] Seq %d can done\n", kv.me, kv.curSeq)
			kv.px.Done(kv.curSeq)
			kv.curSeq++
			DPrintf("[Server %d sync] set the ResChan v=%v\n", kv.me, res)
			op.setResponse(res)
			break
		}
		kv.curSeq++
	}
	DPrintf("[Server %d sync] End\n", kv.me)
}

/**
 *	do paxos to determine the op
 *
 * 	@op
 *	@return the determined op
 */
func (kv *KVPaxos) doPaxos(op Op) Op {
	DPrintf("[Server %d doPaxos] Start seq=%d, op=%s\n", kv.me, kv.curSeq, op.toString())
	kv.px.Start(kv.curSeq, op)
	v := kv.waitPaxosDecided(kv.curSeq).(Op)
	DPrintf("[Server %d doPaxos] End get v=%s\n", kv.me, v.toString())
	return v
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	DPrintf("[Server %d Get] recieved Get request\n", kv.me)
	op := GetOp_new(args.Key, args.Token)
	kv.opChan <- op
	DPrintf("[Server %d Get] wait result\n", kv.me)
	v := op.getResponse()
	DPrintf("[Server %d Get] recieved result %v\n", kv.me, v)
	if err, ok := v.(Err); ok {
		reply.Err = err
	} else {
		reply.Value = v.(string)
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	DPrintf("[Server %d PutAppend] recieved PutAppend request\n", kv.me)
	// Your code here.
	var _op Op
	switch args.Op {
	case "Put":
		_op = PutOp_new(args.Key, args.Value, args.Token)
	case "Append":
		_op = PutAppendOp_new(args.Key, args.Value, args.Token)
	default:
		DPrintf("[Server %d PutAppend] op error\n", kv.me)
	}
	DPrintf("[Server %d PutAppend] PutAppend put op in chan\n", kv.me)
	kv.opChan <- _op
	DPrintf("[Server %d PutAppend] PutAppend wait ResChan\n", kv.me)
	v := _op.getResponse()
	DPrintf("[Server %d PutAppend] get result %v\n", kv.me, v)
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	DPrintf("[Server %d StartServer] start\n", me)
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(&GetOp{})
	gob.Register(&PutOp{})
	gob.Register(&PutAppendOp{})
	gob.Register(&PutAppendArgs{})
	gob.Register(&GetArgs{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.last_rpcs = make(map[string]int)
	kv.data = make(map[string]string)
	kv.opChan = make(chan Op)
	go kv.handleOp()

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.
	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						DPrintf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				DPrintf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
