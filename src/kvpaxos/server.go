package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Args      interface{}
	ResChan   chan interface{}
}

func (this *Op) toString() string {
	switch this.Operation {
	case OP_GET:
		args := this.Args.(*GetArgs)
		return "op:GET key:"+args.Key
	case OP_APPEND:
		args := this.Args.(*PutAppendArgs)
		return "op:APPEND key:"+args.Key+" value:"+args.Value
	case OP_PUT:
		args := this.Args.(*PutAppendArgs)
		return "op:PUT key:"+args.Key+" value:"+args.Value
	default:
	}
	return ""
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

									 // Your definitions here.
	data       map[string]string
	opChan     chan *Op
	curSeq     int
}

func (kv *KVPaxos) loop() {
	fmt.Printf("Server[%d] start loop\n", kv.me)
	for !kv.isdead() {
		select {
		case op := <-kv.opChan:
			kv.doPaxos(op)
		}
	}
}

func (kv *KVPaxos) waitPaxosDecided(seq int) interface{} {
	fmt.Printf("Server[%d] waitPaxosDecided\n", kv.me)
	to := 10 * time.Millisecond
	for {
		fate, v := kv.px.Status(seq)
		if fate == paxos.Decided {
			fmt.Printf("Server[%d] decided value %V\n", kv.me, v)
			return v
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) handleResponse(op *Op) {
	fmt.Printf("Server[%d] handleResponse\n", kv.me)
	switch op.Operation {
	case OP_GET:
		fmt.Printf("Server[%d] handle OP_GET\n", kv.me);
		args := op.Args.(*GetArgs)
		if v, ok := kv.data[args.Key]; ok {
			op.ResChan <- v
		} else {
			op.ResChan <- ErrNoKey
		}
	case OP_PUT:
		fmt.Printf("Server[%d] handle OP_PUT\n", kv.me);
		args := op.Args.(*PutAppendArgs)
		kv.data[args.Key] = args.Value
	case OP_APPEND:
		fmt.Printf("Server[%d] handle OP_APPEND\n", kv.me);
		args := op.Args.(*PutAppendArgs)
		_, ok := kv.data[args.Key]
		if ok {
			kv.data[args.Key] += args.Value
		} else {
			kv.data[args.Key] = args.Value
		}
	default:
		fmt.Printf("[ERROR] op error\n")
	}
}

func (kv *KVPaxos) doPaxos(op *Op) {
	fmt.Printf("Server[%d] doPaxos\n", kv.me)
	if !kv.isdead() {
		kv.px.Start(kv.curSeq, op)
		v := kv.waitPaxosDecided(kv.curSeq).(*Op)
		kv.handleResponse(v)
		kv.curSeq++
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	fmt.Printf("Server[%d] Get\n", kv.me)
	op := &Op{OP_GET, args, make(chan interface{})}
	kv.opChan <- op
	fmt.Printf("Server[%d] wait result\n", kv.me)
	v := <-op.ResChan
	if err, ok := v.(Err); ok {
		reply.Err = err
	} else {
		reply.Value = v.(string)
	}

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	fmt.Printf("Server[%d] PutAppend\n", kv.me)
	// Your code here.
	var op string
	switch args.Op{
	case "Put":
		op = OP_PUT
	case "Append":
		op = OP_APPEND
	default:
		fmt.Printf("Server[%d] op error\n", kv.me)
	}
	fmt.Printf("op = %s\n", op)
	_op := &Op{op, args, make(chan interface{})}
	fmt.Printf("Server[%d] op %V\n", kv.me, _op.Operation)
	kv.opChan <- _op
	fmt.Printf("Server[%d] wait result\n", kv.me)
	<-_op.ResChan
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
	fmt.Printf("Server[%d] start\n", me);
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(&PutAppendArgs{})
	gob.Register(&GetArgs{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.opChan = make(chan *Op)
	go kv.loop()

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
				if kv.isunreliable() && (rand.Int63() % 1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63() % 1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
