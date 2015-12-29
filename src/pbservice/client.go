package pbservice

import "viewservice"
import "net/rpc"

//import "fmt"

import "crypto/rand"
import "math/big"
import "time"
import "log"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	me      string
	primary string
	rpc_id  int
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.me = me
	ck.primary = ck.vs.Primary()
	ck.rpc_id = 1

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("[RPC ERROR] %v", err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	// Your code here.
	ck.rpc_id++
	args := &GetArgs{key, ck.me, ck.rpc_id}
	var reply GetReply
	ok := false
	for !ok {
		ck.primary = ck.vs.Primary()
		log.Printf("[client %s] Get(%s) to [%v] %v times\n", ck.me, key, ck.primary, args.Rpc_id)
		rpc_ok := call(ck.primary, "PBServer.Get", args, &reply)
		if !rpc_ok {
			time.Sleep(viewservice.PingInterval)
			continue
		}
		switch reply.Err {
		case OK:
			ok = true
		case ErrDuplicated:
			ok = true
		case ErrCopyNotFinished:
      log.Printf("[ErrCopyNotFinished][client %v] Primary %v not copy finished\n", ck.me, ck.primary)
    case ErrWrongServer:
      log.Printf("[ErrWrongServer][client %v] %v don't think it's primary\n", ck.me, ck.primary)
		default:
		}
	}
	log.Printf("[client %s] Get(%s)-%v to [%v] SUCCESS\n", ck.me, key, reply.Value, ck.primary)

	return reply.Value
}

/*
 * send a Put or Append RPC
 * Since the net may unreliable, kvserver may execute the append but
 * the rpc call return error.
 */
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// Your code here.
	ck.rpc_id++
	args := &PutAppendArgs{key, value, op, ck.me, ck.rpc_id, PUT_NORMAL}
	var reply PutAppendReply
	ok := false
	for !ok {
		ck.primary = ck.vs.Primary()
		log.Printf("[client %s] PutAppend(%v-%v) to [%v] %v times\n", ck.me, key, value, ck.primary, args.Rpc_id)
		rpc_ok := call(ck.primary, "PBServer.PutAppend", args, &reply)
		if !rpc_ok {
			time.Sleep(viewservice.PingInterval)
			continue
		}
		switch reply.Err {
		case OK:
			ok = true
		case ErrDuplicated:
			ok = true
		case ErrCopyNotFinished:
    case ErrWrongServer:
      log.Printf("[ErrWrongServer][client %v] %v don't think it's primary\n",ck.me, ck.primary)
		case ErrSync:
		default:
		}
	}
	log.Printf("[client %s] PutAppend(%v-%v) to [%v] SECUSS\n", ck.me, key, value, ck.primary)
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
