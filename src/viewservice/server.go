package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	currentView View
	views       map[string]time.Time
	Ack         bool
}

//XXX rpc
// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock() //lock and unlock

	//view-server only fouces on the primary which he think, other clerk can get the
	//newest viewnum through ping.
	if vs.currentView.Primary == args.Me && vs.currentView.Viewnum == args.Viewnum {
		vs.Ack = true
	}
	vs.views[args.Me] = time.Now()
	if args.Viewnum == 0 {
		if vs.currentView.Primary == "" && vs.currentView.Backup == "" {
			//init the view only when p="" and b=""
			vs.currentView.Primary = args.Me
			vs.currentView.Viewnum = 1
		} else if vs.currentView.Primary == args.Me {
			//if currentView contains args.Me, means the clerk is restart.
			//if clerk is work correctly, the viewnum should become big big big,
			//when it crash, it ping(0) since it loose the viewnum.
			//then set the time to infinitely small
			vs.views[args.Me] = time.Time{}
		} else if vs.currentView.Backup == args.Me {
			vs.views[args.Me] = time.Time{}
		}
	}
	reply.View = vs.currentView

	return nil
}

//XXX rpc
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.currentView

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	for kv_server, t := range vs.views {
		if time.Now().Sub(t) > DeadPings*PingInterval {
			delete(vs.views, kv_server)
			if vs.Ack {
				if kv_server == vs.currentView.Primary {
					log.Printf("    Primary [%s] timeout kill\n", kv_server)
					vs.currentView.Primary = ""
				}
				if kv_server == vs.currentView.Backup {
					log.Printf("    Backup [%s] timeout kill\n", kv_server)
					vs.currentView.Backup = ""
				}
			}
		}
	}
	if vs.Ack {
		flag := false
		if vs.currentView.Primary == "" && vs.currentView.Backup != "" {
			vs.currentView.Primary = vs.currentView.Backup
			log.Printf("    Change Backup [%s] to Primary\n", vs.currentView.Primary)
			vs.currentView.Backup = ""
			flag = true
		}
		if vs.currentView.Backup == "" {
			for kv_server, _ := range vs.views {
				if kv_server != vs.currentView.Primary {
					vs.currentView.Backup = kv_server
					log.Printf("    Choose views [%s]to Backup\n", vs.currentView.Backup)
					flag = true
					break
				}
			}
		}
		if flag {
			vs.currentView.Viewnum++
			vs.Ack = false
		}
	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{0, "", ""}
	vs.Ack = false
	vs.views = make(map[string]time.Time)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs) //expose object vs
	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l
	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept() //block utill Dail
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn) //block utill rpc call
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()
	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
