package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"

type PBServer struct {
	mu         sync.Mutex //mutex for data
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk //the PBServer is client of viewserver.
	// Your declarations here.
	data map[string]string
	/*
	 * Record client's rpc request times. In order to filter
	 * duplicated.
	 */
	last_rpc     map[string]int
	myView       viewservice.View
	copyfinished bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	/*
			if pb.last_rpc[args.From] != 0 && args.Rpc_id <= pb.last_rpc[args.From] {
				reply.Err = OK
		    reply.Value = pb.data[args.Key]
				log.Printf("[DUPLICATED][server %v] respons Get(%v) from %v server_rpc_id:%v, client_rpc_id:%v",
					pb.me, args.Key, args.From, pb.last_rpc[args.From], args.Rpc_id)
				return nil
			}
	*/
	if !pb.copyfinished {
		reply.Err = ErrCopyNotFinished
		return nil
	}
	reply.Value = pb.data[args.Key]
	reply.Err = OK
	pb.last_rpc[args.From] = args.Rpc_id
	log.Printf("[server %v] respons Get(%v)-%v from %v", pb.me, args.Key, reply.Value, args.From)

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	//filter all duplicated request
	if pb.last_rpc[args.From] != 0 && args.Rpc_id <= pb.last_rpc[args.From] {
		reply.Err = ErrDuplicated
		log.Printf("[DUPLICATED][server %v] respons PutAppend(%v)-%v from %v server_rpc_id:%v, client_rpc_id:%v",
			pb.me, args.Key, args.Value, args.From, pb.last_rpc[args.From], args.Rpc_id)
		return nil
	}
	/*
			 * Sync to the backup.Primary should forward the request to
		   * Backup through RPC. And the Primary no need to ensure sync
		   * success, just throw the error to the client.
	*/
	if pb.myView.Backup != "" && pb.myView.Backup != pb.me {
		log.Printf("[Sync] [%v] sync to backup [%v]", pb.me, pb.myView.Backup)
		var reply_backup PutAppendReply
		ok := call(pb.myView.Backup, "PBServer.PutAppend", args, &reply_backup)
		if !ok {
			log.Printf("[ERROR] [%v] sync to backup [%v]", pb.me, pb.myView.Backup)
			return errors.New("Sync error")
		}
	}

	switch args.Op {
	case "Put":
		pb.data[args.Key] = args.Value
		reply.Err = OK
	case "Append":
		pb.data[args.Key] = pb.data[args.Key] + args.Value
		reply.Err = OK
	default:
	}
	pb.last_rpc[args.From] = args.Rpc_id
	log.Printf("[server %v] PutAppend(%v)-%v from %v SECUSS", pb.me, args.Key, args.Value, args.From)

	return nil
}

//XXX
func (pb *PBServer) Copy(args *CopyArgs, reply *CopyReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//check wheather match viewserver
	if pb.me != pb.myView.Backup {
		return errors.New("Not current Backup!")
	}
	log.Printf("[PBServer %v] recieve data from [%v]", pb.me, args.From)
	for k, v := range args.Data {
		pb.data[k] = v
	}
	for k, v := range args.Last_rpc {
		pb.last_rpc[k] = v
	}
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	view, err := pb.vs.Ping(pb.myView.Viewnum)
	if err != nil {
		return
	}

	flag := (pb.me == view.Primary &&
		view.Backup != "" && view.Backup != pb.myView.Backup)
	pb.myView = view
	if flag {
		log.Printf("[PBServer %v] Backup mismatch, send data to [%v]", pb.me, pb.myView.Backup)
		pb.copyfinished = false
		go func() {
			args := &CopyArgs{pb.data, pb.me, pb.last_rpc}
			var reply CopyReply
			for {
				ok := call(pb.myView.Backup, "PBServer.Copy", args, &reply)
				if !ok {
					time.Sleep(viewservice.PingInterval)
					continue
				} else {
					pb.copyfinished = true
					break
				}
			}
		}()
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.data = make(map[string]string)
	pb.myView = viewservice.View{0, "", ""}
	pb.copyfinished = true
	pb.last_rpc = make(map[string]int)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
