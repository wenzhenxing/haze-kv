package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

//import "errors"
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Status struct {
	status       Fate
	bidding      int
	accept_value interface{}
}

func Status_new() *Status {
	return &Status{Pending, -1, nil}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	min_instance int
	max_instance int
	done         int
	status       map[int]*Status
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

const (
	OK = iota + 1
	Reject
	NotMajority
)

type PrepareArgs struct {
	From    int
	Seq     int
	Bidding int
}

type PrepareReply struct {
	Err            int
	Accept_bidding int
	Accept_Value   interface{}
}

func (px *Paxos) Prepare_handler(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	px.Prepare_handler_local(args, reply)
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Prepare_handler_local(args *PrepareArgs, reply *PrepareReply) {
	fmt.Printf("[Instance %v Prepare_handler] [Paxos-%v] Do prepare request bidding:%v from:[Paxos %v]\n", args.Seq, px.me, args.Bidding, args.From)
	s, ok := px.status[args.Seq]
	if !ok {
		px.status[args.Seq] = Status_new()
		s = px.status[args.Seq]
	}
  fmt.Printf("[Instance %v Prepare_handler][Paxos-%v] prev proposal bidding:%v value:%v\n", args.Seq, px.me, s.bidding, s.accept_value)
	if args.Bidding > s.bidding {
    fmt.Printf("[Instance %v Prepare_handler][Paxos-%v] requtest bidding %v > prev bidding %v\n", args.Seq, px.me,args.Bidding, s.accept_value)
		s.bidding = args.Bidding
	  reply.Accept_bidding = s.bidding
	  reply.Accept_Value = s.accept_value
		reply.Err = OK
	} else {
    reply.Accept_bidding = s.bidding
		reply.Err = Reject
	}
}

/*
 * Broadcast the prepare request to all Acceptor
 */
func (px *Paxos) Prepare(seq int, bidding int, v interface{}) (interface{}, int) {
	fmt.Printf("[Instance %v Prepare_Start Paxos-%v] Broadcast Prepare bidding:%v value:%v\n", seq, px.me, bidding, v)
	recieved_num := 0
	cur_value := v
	max_bidding := bidding
	for idx, p := range px.peers {
		args := &PrepareArgs{px.me, seq, bidding}
		var reply PrepareReply
		if idx == px.me {
			px.mu.Lock()
			px.Prepare_handler_local(args, &reply)
			px.mu.Unlock()
		} else {
			call(p, "Paxos.Prepare_handler", args, &reply)
		}
		switch reply.Err {
		case OK:
			recieved_num++
        fmt.Printf("[Instance %v Prepare_recieve Paxos-%v] get a promise. accept_value:%v accept_bidding:%v\n", seq, idx, reply.Accept_Value, reply.Accept_bidding)
			if reply.Accept_Value != nil && reply.Accept_bidding > max_bidding {
				max_bidding = reply.Accept_bidding
				cur_value = reply.Accept_Value
			}
		case Reject:
			fmt.Printf("[Instance %v Prepare_recieve Paxos-%v] Reject prepare since it has a high bidding %v than %v\n", seq, idx, reply.Accept_bidding, bidding)
      //return nil, Reject
		default:
		}
	}
	if recieved_num > len(px.peers)/2 {
		if cur_value != v {
			fmt.Printf("[Instance %v Prepare_Betray Paxos-%v] Change Value %v to %v\n", seq, v, px.me, cur_value)
		}
		return cur_value, OK
	} else {
		return nil, NotMajority
	}
}

type AcceptArgs struct {
	From    int
	Seq     int
	Bidding int
	Value   interface{}
}

type AcceptReply struct {
	Err     int
	Bidding int
}

func (px *Paxos) Accept_handler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	px.Accept_handler_local(args, reply)
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Accept_handler_local(args *AcceptArgs, reply *AcceptReply) {
	fmt.Printf("[Instance %v Accept_handler Paxos-%v] bidding:%v value:%v \n", args.Seq, px.me, args.Bidding, args.Value)
	s, ok := px.status[args.Seq]
	if !ok {
		s = Status_new()
		px.status[args.Seq] = s
	}
  reply.Bidding = s.bidding
  if args.Bidding >= s.bidding {
    reply.Err = OK
		s.bidding = args.Bidding
		s.accept_value = args.Value
	}else {
    reply.Err = Reject
  }
}

func (px *Paxos) Accept(seq int, bidding int, v interface{}) (interface{}, int) {
	fmt.Printf("[Instance %v Accept_Start][Paxos %v] Broadcast to all bidding:%v value:%v\n", seq, px.me, bidding, v)
	recieved_num := 0
	for idx, p := range px.peers {
		args := &AcceptArgs{px.me, seq, bidding, v}
		var reply AcceptReply
		if idx == px.me {
			px.mu.Lock()
			px.Accept_handler_local(args, &reply)
			px.mu.Unlock()
		} else {
			call(p, "Paxos.Accept_handler", args, &reply)
		}
		switch reply.Err {
		case OK:
			recieved_num++
      //if reply.Bidding > bidding {
      //  return nil, Reject
      //}
    case Reject:
			//return nil, Reject
		default:
		}
	}
	if recieved_num > len(px.peers)/2 {
		return v, OK
	}
	return nil, NotMajority
}

type LearnArgs struct {
	From    int
	Seq     int
	Bidding int
	Value   interface{}
}

type LearnReply struct {
	Err int
}

func (px *Paxos) Learn_handler(args *LearnArgs, reply *LearnReply) error {
	px.mu.Lock()
	px.Learn_handler_local(args, reply)
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Learn_handler_local(args *LearnArgs, reply *LearnReply) {
	fmt.Printf("[Instance %v Learn_handler Paxos-%v] bidding:%v, value:%v \n", args.Seq, px.me, args.Bidding, args.Value)
	s, ok := px.status[args.Seq]
	if !ok {
		px.status[args.Seq] = Status_new()
		s = px.status[args.Seq]
	}
	s.bidding = args.Bidding
	s.accept_value = args.Value
	s.status = Decided
	if args.Seq > px.max_instance {
		fmt.Printf("[Instance %v Learn_handler Paxos-%v] Change the max_instance %v to %v\n", args.Seq, px.me, px.max_instance, args.Seq)
		px.max_instance = args.Seq
	}

	reply.Err = OK
}

func (px *Paxos) Learn(seq int, bidding int, v interface{}) int {
	fmt.Printf("[Instance %v Learn_Start Paxos-%v]Broadcast learn to all bidding:%v, value:%v\n", seq, px.me, bidding, v)
	recieved_num := 0
	for idx, p := range px.peers {
		args := &LearnArgs{px.me, seq, bidding, v}
		var reply LearnReply
		if idx == px.me {
			px.mu.Lock()
			px.Learn_handler_local(args, &reply)
			px.mu.Unlock()
		} else {
			call(p, "Paxos.Learn_handler", args, &reply)
		}
		if reply.Err == OK {
			recieved_num++
		}
	}
	if recieved_num == len(px.peers) {
		return OK
	}
	return NotMajority
}

func (px *Paxos) Instance(seq int, bidding int, v interface{}) Fate {
  fmt.Printf("[Instance %v Start Paxos-%v] bidding:%v value:%v\n", seq, px.me, bidding, v)
	//check
	px.mu.Lock()
	if seq < px.min_instance {
		px.mu.Unlock()
		return Forgotten
	}
	s, ok := px.status[seq]
	if ok && s.status == Decided {
		px.mu.Unlock()
		return Decided
	}
	if s.bidding > bidding {
		fmt.Printf("[Instance %v] Paxos-%v Reject. current bidding:%v, your bidding:%v\n", seq, px.me, s.bidding, bidding)
		px.mu.Unlock()
		return Pending
	}
	px.mu.Unlock()
	value, err_p := px.Prepare(seq, bidding, v)
	switch err_p {
	case OK:
		fmt.Printf("[Instance %v Prepare_OK Paxos-%v] Proposal value:%v\n", seq, px.me, value)
	case NotMajority:
		fmt.Printf("[Instance %v Prepare_FAIL Paxos-%v] Not recieve enough promise\n", seq, px.me)
		return Pending
	case Reject:
    return Pending
	default:
		panic("impossible")
	}
	choosen_value, err_a := px.Accept(seq, bidding, value)
	switch err_a {
	case OK:
		fmt.Printf("[Instance %v Accept_OK Paxos-%v]get Majority Choosen value:%v\n", seq, px.me, choosen_value)
	case NotMajority:
		fmt.Printf("[Instance %v Accept_No Paxos-%v] Not recieve enough acception\n", seq, px.me)
		return Pending
	case Reject:
		fmt.Printf("[Instance %v Accept_Reject Paxos-%v]Acceptor alreay has high bidding\n", seq, px.me)
		return Pending
	default:
		panic("impossible")
	}
	err_l := px.Learn(seq, bidding, choosen_value)
	switch err_l {
	case OK:
		fmt.Printf("[Instance %v Learn_OK Paxos-%v]All Acceptor learn success\n", seq, px.me)
	case NotMajority:
		fmt.Printf("[Instance %v Learn_No Paxos-%v] Not all Acceptor learn success.Need retry ?\n", seq, px.me)
	default:
		panic("impossible")
	}
	return Decided
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		px.mu.Lock()
		if px.status[seq] == nil {
			px.status[seq] = Status_new()
		}
		if px.status[seq].status == Decided {
			return
		}
		px.mu.Unlock()
		bidding := 0
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			decided := px.Instance(seq, bidding, v)
			if px.isdead() {
				fmt.Printf("[Paxos %v] Dead!!!!!\n", px.me)
				break
			}
			if decided == Decided {
				fmt.Printf("[Instance %v][Paxos %v]Decided\n", seq, px.me)
				return
			}
			time.Sleep(time.Duration(r.Intn(100)) * time.Millisecond)
			bidding++
		}
	}()
}

type GCArgs struct {
	From    int
	Seq     int
	Min_seq int
}

type GCReply struct {
	Err int
}

type DoneArgs struct {
	From    int
	Seq     int
	Min_seq int
}

type DoneReply struct {
	Err      int
	Done_seq int
}

func (px *Paxos) DoGC(args *GCArgs, reply *GCReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.DoGC_local(args, reply)
	return nil
}

func (px *Paxos) DoGC_local(args *GCArgs, reply *GCReply) {
	if px.min_instance < args.Min_seq {
		px.min_instance = args.Min_seq
	}
	for k, _ := range px.status {
		if k < px.min_instance {
			delete(px.status, k)
		}
	}
	reply.Err = OK
	fmt.Printf("[GC] [Paxos %v] do gc delete seq < %v\n", px.me, px.min_instance)
}

func (px *Paxos) GetDone(args *DoneArgs, reply *DoneReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Done_seq = px.done
	//px.min_instance = px.done+1
	reply.Err = OK
	fmt.Printf("[Done] [Paxos %v] My done seq:%v\n", px.me, px.done)
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	fmt.Printf("[Paxos %v] Done seq:%v\n", px.me, seq)
	if seq < px.min_instance {
		fmt.Printf("[Done][Paxos %v] %v < %v, omit\n", px.me, seq, px.min_instance)
		return
	}
	if seq > px.max_instance {
		fmt.Printf("[Done][Paxos %v] set seq by max_instance:%v\n", px.me, px.max_instance)
		seq = px.max_instance
	}
	if seq < px.done {
		fmt.Printf("[Done][Paxos %v] %v < %v, omit\n", px.me, seq, px.done)
		return
	}
	px.done = seq
	min := px.done
	get_all := true
	fmt.Printf("[Done] [Paxos %v] Broadcast GetDone to all\n", px.me)
	for idx, p := range px.peers {
		if idx == px.me {
			continue
		}
		args := &DoneArgs{idx, seq, px.done}
		var reply DoneReply
		rpc_ok := call(p, "Paxos.GetDone", args, &reply)
		get_all = get_all && rpc_ok
		if rpc_ok {
			if reply.Done_seq < min {
				min = reply.Done_seq
			}
		}
	}
	if get_all {
		px.min_instance = min + 1
		fmt.Printf("[Done] Minium is %v\n", px.min_instance)
	} else {
		fmt.Printf("[Done] Not recieve all response\n")
		return
	}
	//GC
	get_all = true
	for idx, p := range px.peers {
		args := &GCArgs{idx, seq, px.min_instance}
		var reply GCReply
		if idx == px.me {
			px.DoGC_local(args, &reply)
		} else {
			rpc_ok := call(p, "Paxos.DoGC", args, &reply)
			get_all = get_all && rpc_ok
		}
	}
	if !get_all {
		fmt.Printf("[GC] Not gc all\n")
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.max_instance
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.min_instance
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq < px.min_instance {
		return Forgotten, nil
	}
	s, ok := px.status[seq]
	if !ok {
		px.status[seq] = Status_new()
	} else {
		return s.status, s.accept_value
	}
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.min_instance = 0
	px.max_instance = 0
	px.done = 0
	px.status = make(map[int]*Status)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
