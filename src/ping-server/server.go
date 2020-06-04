package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AccessPatterns struct {
	t        time.Time
	prevPing uint
}

type ViewServer struct {
	mux           sync.Mutex
	l             net.Listener
	dead          bool
	me            string
	isInit        bool
	isPrimaryInit bool
	view          View
	pings         map[string]*AccessPatterns
	Dlog          *log.Logger
}

func (vs *ViewServer) createDebugger() *os.File {
	t := time.Now()
	timeInt := 60*60*t.Hour() + 60*t.Minute() + t.Second()
	currTime := strconv.Itoa(timeInt)
	me := strings.ReplaceAll(vs.me, "/", "")
	debug := fmt.Sprintf("./debug_viewserver_%s_%s.txt", me, currTime)
	if _, err := os.Stat(debug); err == nil {
		os.Remove(debug)
	}
	f, err := os.Create(debug)
	if err != nil {
		panic(err)
	}
	vs.Dlog = log.New(f, "", 3)
	return f
}

func (vs *ViewServer) onInit() {
	vs.Dlog.Println("Step 2: Intializing viewserver data")
	vs.isInit = true
	vs.isPrimaryInit = false
	vs.view = View{Primary: "", Backup: "", Idle: "", Viewnum: 0}
	vs.pings = make(map[string]*AccessPatterns)
}

func (vs *ViewServer) initPrimary(portname string, pingNum uint) {
	vs.Dlog.Println("Step 3: Intializing primary server data")
	vs.mux.Lock()
	primInit := vs.isPrimaryInit
	vs.mux.Unlock()
	if primInit == false {
		vs.mux.Lock()
		vs.view.Primary = portname
		vs.pings[vs.view.Primary] = &AccessPatterns{t: time.Now(), prevPing: pingNum}
		vs.isPrimaryInit = true
		vs.view.Viewnum++
		vs.mux.Unlock()
	}
	// NOTE: used to panic if to primaries tried to init
	//else {
	// panic("Attempt to init primary twice.")
	//return
	//}
}

func (vs *ViewServer) ackHold() bool {
	_, ok := vs.pings[vs.view.Primary]
	if ok == false || vs.view.Primary == "" {
		panic("Primary is empty or for some other reason not in pings map")
	}
	vs.mux.Lock()
	var x bool
	if vs.view.Viewnum == vs.pings[vs.view.Primary].prevPing {
		x = false
	} else {
		vs.Dlog.Printf("Ack Hold: viewnum: %d is not equal to primary prev ping viewnum: %d", vs.view.Viewnum, vs.pings[vs.view.Primary].prevPing)
		x = true
	}
	vs.mux.Unlock()
	return x
}

func (vs *ViewServer) isDead(portname string) bool {
	var x bool
	vs.mux.Lock()
	accessPattern, ok := vs.pings[portname]
	if ok == false || portname == "" {
		panicStr := fmt.Sprintf("Value %s not in pings map", portname)
		panic(panicStr)
	}
	tNow := time.Now()
	tDur := tNow.Sub(accessPattern.t)
	vs.Dlog.Println("t_durr", tDur, "against", PingInterval*DeadPings)
	if tDur <= DeadPings*PingInterval {
		x = false
	} else {
		x = true
	}
	if x == false {
		vs.Dlog.Printf("Status: %s is alive\n", portname)
	} else {
		vs.Dlog.Printf("Status: %s is dead\n", portname)
	}
	vs.mux.Unlock()
	return x
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.Dlog.Println("Ping called with ping: ", args, " and state: ", vs.view)
	portname, viewnum := args.Me, args.Viewnum
	isIdleUsable := vs.isIdleUsable()
	vs.mux.Lock()
	_, ok := vs.pings[portname]
	vs.Dlog.Println(ok, vs.pings[portname], viewnum)
	vs.mux.Unlock()
	if vs.isPrimaryInit == false {
		vs.Dlog.Println("Ping condition 1")
		vs.initPrimary(portname, viewnum)
	} else if ok == true && vs.pings[portname] != nil && vs.pings[portname].prevPing == viewnum {
		vs.Dlog.Println("Ping condition 2")
		vs.mux.Lock()
		vs.pings[portname].t = time.Now()
		vs.mux.Unlock()
	} else if portname == vs.view.Primary {
		vs.Dlog.Println("Ping condition 3")
		vs.mux.Lock()
		if viewnum == 0 && vs.view.Backup != "" {
			vs.Dlog.Println("Ping condition 3.1")
			vs.pings[vs.view.Primary] = nil
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
			if isIdleUsable == true {
				vs.Dlog.Println("Ping condition 3.1.1")
				vs.view.Backup = vs.view.Idle
				vs.view.Idle = ""
			}
		} else {
			vs.Dlog.Println("Ping condition 3.2")
			vs.pings[vs.view.Primary].t = time.Now()
			vs.pings[vs.view.Primary].prevPing = viewnum
		}
		vs.mux.Unlock()
	} else if portname == vs.view.Backup {
		vs.Dlog.Println("Ping condition 4")
		isIdleUsable := vs.isIdleUsable()
		vs.mux.Lock()
		if viewnum == 0 {
			vs.Dlog.Println("Ping condition 4.1")
			vs.pings[vs.view.Backup] = nil
			vs.view.Backup = ""
			if isIdleUsable == true && vs.view.Primary != "" {
				vs.Dlog.Println("Ping condition 4.1.2")
				vs.view.Backup = vs.view.Idle
				vs.view.Idle = ""
				vs.view.Viewnum++
			}
		} else {
			vs.Dlog.Println("Ping condition 4.2")
			vs.pings[vs.view.Backup].t = time.Now()
			vs.pings[vs.view.Backup].prevPing = viewnum
		}
		vs.mux.Unlock()
	} else {
		vs.Dlog.Println("Ping condition 5")
		isAckHold := vs.ackHold()
		vs.mux.Lock()
		if vs.view.Backup == "" && isAckHold == false {
			vs.Dlog.Println("Ping condition 5.1")
			vs.view.Backup = portname
			vs.pings[vs.view.Backup] = &AccessPatterns{t: time.Now(), prevPing: viewnum}
			vs.view.Viewnum += 1
		} else if vs.view.Idle == "" && isAckHold == false {
			vs.Dlog.Println("Ping condition 5.2")
			vs.view.Idle = portname
			vs.pings[vs.view.Idle] = &AccessPatterns{t: time.Now(), prevPing: viewnum}
		} else if vs.view.Idle != "" {
			vs.Dlog.Println("Ping condition 5.2")
			vs.pings[vs.view.Idle].t = time.Now()
			vs.pings[vs.view.Idle].prevPing = viewnum
		}
		vs.mux.Unlock()
	}
	vs.mux.Lock()
	reply.View = vs.view
	vs.Dlog.Println("Ping condition 6: End with reply: ", reply)
	vs.mux.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mux.Lock()
	reply.View = vs.view
	vs.mux.Unlock()
	return nil
}

func (vs *ViewServer) isIdleUsable() bool {
	vs.mux.Lock()
	idle := vs.view.Idle
	vs.mux.Unlock()
	x := false
	if idle != "" && vs.isDead(vs.view.Idle) == false {
		x = true
	}
	return x
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.Dlog.Println("Tick condition 1")
	if vs.isPrimaryInit == true && vs.ackHold() == false {
		vs.Dlog.Println("Tick condition 1.1")
		if vs.view.Primary != "" && vs.isDead(vs.view.Primary) == true && vs.view.Backup != "" && vs.isDead(vs.view.Backup) == false {
			vs.Dlog.Println("Tick condition 1.1.1")
			isIdleUsable := vs.isIdleUsable()
			vs.mux.Lock()
			vs.pings[vs.view.Primary] = nil
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
			if isIdleUsable == true {
				vs.Dlog.Println("Tick Condition 1.1.1.1")
				vs.view.Backup = vs.view.Idle
				vs.view.Idle = ""
			}
			vs.view.Viewnum++
			vs.mux.Unlock()
		} else if vs.view.Backup != "" && vs.isDead(vs.view.Backup) == true {
			vs.Dlog.Println("Tick Condition 1.1.2")
			isIdleUsable := vs.isIdleUsable()
			var isIdleDead bool
			if isIdleUsable == true {
				isIdleDead = vs.isDead(vs.view.Idle)
			} else {
				isIdleDead = false
			}
			vs.mux.Lock()
			vs.pings[vs.view.Backup] = nil
			vs.view.Backup = ""
			if isIdleUsable == true {
				vs.Dlog.Println("Tick Condition 1.1.2.1")
				vs.view.Backup = vs.view.Idle
				vs.view.Idle = ""
				vs.view.Viewnum++
			} else if vs.view.Idle != "" && isIdleDead == true {
				vs.Dlog.Println("Tick Condition 1.1.2.2")
				vs.pings[vs.view.Idle] = nil
				vs.view.Idle = ""
			}
			vs.mux.Unlock()
		} else if vs.view.Idle != "" && vs.isDead(vs.view.Idle) == true {
			vs.mux.Lock()
			vs.Dlog.Println("Tick Condition 1.1.3")
			vs.pings[vs.view.Idle] = nil
			vs.view.Idle = ""
			vs.mux.Unlock()
		}
	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	f := vs.createDebugger()
	vs.Dlog.Println("Step 1: Starting viewserver")
	go func(file *os.File) {
		for vs.dead == false {
			time.Sleep(PingInterval)
		}
		f.Close()
	}(f)
	vs.onInit()
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

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
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
