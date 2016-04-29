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
	stop         bool
	rpc_id       int //for rpc copy
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	//check wrong server
	if pb.myView.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}
	//check the connection between server and viewserver.
	if pb.stop {
		reply.Err = ErrUnReliable
		return nil
	}
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
	//check wrong server
	switch args.Kind {
	case PUT_NORMAL:
		if pb.myView.Primary != pb.me {
			reply.Err = ErrWrongServer
			return nil
		}
	case PUT_SYNC:
		if pb.myView.Backup != pb.me {
			reply.Err = ErrWrongServer
			return errors.New("ErrWrongServer")
		}
	default:
	}
	// check connect between server and viewserver
	if pb.stop {
		reply.Err = ErrUnReliable
		return nil
	}
	// filter all duplicated request
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
		sync_args := &PutAppendArgs{args.Key, args.Value, args.Op, args.From, args.Rpc_id, PUT_SYNC}
		var reply_backup PutAppendReply
		ok := call(pb.myView.Backup, "PBServer.PutAppend", sync_args, &reply_backup)
		if !ok {
			log.Printf("[ERROR] [%v] sync to backup [%v]", pb.me, pb.myView.Backup)
			reply.Err = ErrSync
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
	// filter the duplicated request
	if pb.last_rpc[args.From] != 0 && args.Rpc_id <= pb.last_rpc[args.From] {
		reply.Err = ErrDuplicated
		log.Printf("[DUPLICATED][server %v] Copy request duplicated primary_rpc_id:%v, backup_rpc_id:%v",
			pb.me, args.Rpc_id, pb.last_rpc[args.From])
		return nil
	}

	// check wheather match viewserver
	if pb.me != pb.myView.Backup {
		reply.Err = ErrWrongServer
		return nil
	}
	for k, v := range args.Data {
		pb.data[k] = v
	}
	for k, v := range args.Last_rpc {
		pb.last_rpc[k] = v
	}
	reply.Err = OK
	log.Printf("[server %v] recieve data from [%v] SUCCESS", pb.me, args.From)
	pb.last_rpc[args.From] = args.Rpc_id
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
		pb.stop = true
		return
	}
	pb.stop = false

	flag := (pb.me == view.Primary &&
		view.Backup != "" && view.Backup != pb.myView.Backup)
	pb.myView = view
	if flag {
		pb.copyfinished = false
		go func() {
			pb.rpc_id++
			args := &CopyArgs{pb.data, pb.me, pb.last_rpc, pb.rpc_id}
			var reply CopyReply
			ok := false
			for !ok {
				log.Printf("[server %v] Backup mismatch, send data to [%v] %v times", pb.me, pb.myView.Backup, args.Rpc_id)
				rpc_ok := call(pb.myView.Backup, "PBServer.Copy", args, &reply)
				if !rpc_ok {
					time.Sleep(viewservice.PingInterval)
					continue
				}
				switch reply.Err {
				case OK:
					pb.copyfinished = true
					ok = true
				case ErrDuplicated:
					pb.copyfinished = true
					ok = true
				case ErrWrongServer:
					log.Printf("[ErrWrongServer] %v don't this it's buckup", pb.myView.Backup)
				default:
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
	pb.stop = false
	pb.last_rpc = make(map[string]int)
	pb.rpc_id = 0

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
