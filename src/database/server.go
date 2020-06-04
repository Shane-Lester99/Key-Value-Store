package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

func (pb *PBServer) createDebugger(nameExt string) *os.File {
	t := time.Now()
	timeInt := 60*60*t.Hour() + 60*t.Minute() + t.Second()
	currTime := strconv.Itoa(timeInt)
	nameExt = strings.ReplaceAll(nameExt, "/", "")
	debug := fmt.Sprintf("./%sdebug_pbserver_%s.txt", currTime, nameExt)
	if _, err := os.Stat(debug); err == nil {
		os.Remove(debug)
	}
	f, err := os.Create(debug)
	if err != nil {
		panic(err)
	}
	pb.Dlog = log.New(f, "", 3)
	return f
}

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

const (
	Reboot  = "Reboot"
	Primary = "Primary"
	Backup  = "Backup"
	Idle    = "Idle"
)

type State string

type View struct {
	Viewnum uint
	Primary string
	Backup  string
	Idle    string
}

type ViewCard struct {
	View     *View
	prevPing time.Time
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	mux               sync.Mutex
	state             State
	db                map[string]string
	prevReq           map[string]*ReqCard
	backupInit        bool
	currView          *ViewCard
	res               chan *ReqCard
	Dlog              *log.Logger
	CurrentPutRequest bool
	CurrentGetRequest bool
}

func (pb *PBServer) makeReplicaPrevReq(prevReq map[string]*ReqCard) map[string]*ReqCard {
	newMap := make(map[string]*ReqCard)
	pb.mux.Lock()
	for key, value := range prevReq {
		newMap[key] = value
	}
	pb.mux.Unlock()
	return newMap
}

func (pb *PBServer) makeReplicaDb(db map[string]string) map[string]string {
	newMap := make(map[string]string)
	pb.mux.Lock()
	for key, value := range db {
		newMap[key] = value
	}
	pb.mux.Unlock()
	return newMap
}

func (pb *PBServer) buildPrevReqKey(serverTag string, methodName Method, reqNum int64, key string, value string) string {
	return fmt.Sprintf("%s-%s-%d-(%s,%s)", serverTag, methodName, reqNum, key, value)
}

func (pb *PBServer) resetServer() {
	pb.Dlog.Printf("Resetting Server %s", pb.me)
	pb.mux.Lock()
	pb.state = Reboot
	pb.mux.Unlock()
	pb.resetMaps()
	pb.resetView()
	pb.Dlog.Printf("Finished resetting server %s", pb.me)
}

// func (pb *PBServer) viewExpired() bool {
// 	pb.mux.Lock()
// 	pb.Dlog.Printf("Server %s with state %s and view %+v is checking if expired.",
// 		pb.me, pb.state, pb.currView)
// 	view := pb.currView.View
// 	pb.mux.Unlock()
// 	if view != nil {
// 		pb.mux.Lock()
// 		tNow := time.Now()
// 		tDur := tNow.Sub(pb.currView.prevPing)
// 		var ans bool
// 		if tDur <= DeadPings*PingInterval {
// 			pb.Dlog.Printf("View with server %s hasn't expired.", pb.me)
// 			ans = false
// 		} else {
// 			pb.Dlog.Printf("View with server %s has expired. Oops!", pb.me)
// 			ans = true
// 		}
// 		pb.mux.Unlock()
// 		return ans
// 	} else {
// 		pb.Dlog.Printf("View with server %s hasn't expired, view is still nil.", pb.me)
// 		return false
// 	}
// }
//
// func (pb *PBServer) reprocessPendingRequests() {
// 	pb.mux.Lock()
// 	for _, v := range pb.prevReq {
// 		if v.Processed == false {
// 			pb.res <- v
// 		}
// 	}
// 	pb.mux.Unlock()
// }

func (pb *PBServer) initBackupAsMe() {
	pb.mux.Lock()
	currView := pb.currView
	pb.mux.Unlock()
	count := 1
	for {
		if currView == nil {
			pb.Dlog.Printf("Init Backup 1: from %s to %s. I am %s. Attempt %d", pb.currView.View.Primary, pb.currView.View.Backup, pb.state, count)
			count++
			time.Sleep(PingInterval)
			continue
		}
		fargs := &ForwardArgs{ReqType: InitBackup, Database: nil, PrevReq: nil}
		freply := &ForwardReply{Err: "", Database: nil, PrevReq: nil}
		for i := 0; i < DeadPings; i++ {
			pb.Dlog.Println("Call made")
			ok := call(currView.View.Primary, "PBServer.Forward", fargs, freply)
			if ok == false || freply.Err == ErrWrongServer {
				break
			} else if freply.Err == OK {
				replicaDB := pb.makeReplicaDb(freply.Database)
				replicaPrevReq := pb.makeReplicaPrevReq(freply.PrevReq)
				pb.mux.Lock()
				pb.db = replicaDB
				pb.prevReq = replicaPrevReq
				pb.Dlog.Printf("Init Backup 2: %s in state %s now has db %+v and prevReq %+v. Init finished for backup.",
					pb.me, pb.state, pb.db, pb.prevReq)
				pb.mux.Unlock()
				return
			}
			time.Sleep(PingInterval)
		}

	}
}

func (pb *PBServer) handleBackupForward(fargs *ForwardArgs, freply *ForwardReply) {
	pb.Dlog.Printf("Case 1: handleBackupForward launched for server %s", pb.me)
	for pb.backupInit == false {
		pb.Dlog.Printf("Case 2: handleBackupForward: wait on pb.backupInit")
		time.Sleep(PingInterval)
	}
	badCallCount := 0
	for {
		pb.Dlog.Printf("Case 3: handleBackupForward: calling backup and forwarding")
		pb.mux.Lock()
		backup := pb.currView.View.Backup
		pb.mux.Unlock()
		if backup == "" {
			break
		}
		for i := 0; i < DeadPings; i++ {
			pb.mux.Lock()
			pb.Dlog.Println("View: ", pb.currView.View.Primary, pb.currView.View.Backup, pb.currView.View.Idle, pb.currView.View.Viewnum)
			pb.mux.Unlock()
			pb.Dlog.Printf("Case 4: handleBackupForward: attempting call")
			pb.Dlog.Println("Call made")
			ok := call(backup, "PBServer.Forward", fargs, freply)
			if ok == false {
				pb.Dlog.Println("Case 4.1.1: handleBackupForward: bad call ", badCallCount)
				if badCallCount == 15 {
					return
				}
				badCallCount++
			} else if freply.Err == ErrWrongServer {
				pb.Dlog.Printf("Case 4.1.2: handleBackupForward: error, break and try again")
				break
			} else if freply.Err == OK {
				pb.Dlog.Printf("Case 4.2: handleBackupForward: success")
				return
			}
			time.Sleep(PingInterval)
		}
	}
}

func (pb *PBServer) removeOldRequest(key string) {
	// TODO: Fill in if large maps become an issue for garbage collection
	// pb.Dlog.Printf("Removing old request %s", key)
	// time.Sleep(3 * time.Second)
	// pb.mux.Lock()
	// delete(pb.prevReq, key)
	// pb.mux.Unlock()
}

func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
	pb.Dlog.Printf("Case 1 Forward: server %s", pb.me)
	pb.mux.Lock()
	state := pb.state
	reqType := args.ReqType
	pb.mux.Unlock()
	if state != Backup && reqType != InitBackup {
		pb.Dlog.Printf("Case 2 Forward: %s is not backup or %s isn't to init backup", state, reqType)
		reply.Err = ErrWrongServer
	} else {
		if reqType == DbUpdate {
			pb.Dlog.Printf("Case 3 DBUpdate requested")
			replicaDB := pb.makeReplicaDb(args.Database)
			replicaPrevReq := pb.makeReplicaPrevReq(args.PrevReq)
			pb.mux.Lock()
			pb.db = replicaDB
			pb.prevReq = replicaPrevReq
			pb.prevReq[args.Key].Processed = true
			pb.mux.Unlock()
			reply.Err = OK
		} else if reqType == PrevReqUpdate {
			pb.Dlog.Printf("Case 3 PrevReqUpdate requested")
			replicaPrevReq := pb.makeReplicaPrevReq(args.PrevReq)
			pb.mux.Lock()
			pb.prevReq = replicaPrevReq
			pb.mux.Unlock()
			reply.Err = OK
		} else if reqType == InitBackup {
			if state != Primary {
				reply.Err = ErrWrongServer
			}
			replicaDB := pb.makeReplicaDb(args.Database)
			replicaPrevReq := pb.makeReplicaPrevReq(args.PrevReq)
			pb.mux.Lock()
			reply.Database = replicaDB
			reply.PrevReq = replicaPrevReq
			pb.backupInit = true
			reply.Err = OK
			pb.mux.Unlock()
		} else {
			panic("Our forwarding logic has no correct method. Exit.")
		}
	}
	return nil
}

func (pb *PBServer) handleGet(args *GetArgs, reply *GetReply) {
	pb.Dlog.Printf("Case handleGet 1: Get request to server %s requested", pb.me)
	key := pb.buildPrevReqKey(args.Sender, args.ReqType, args.ReqNum, args.Key, "")
	pb.mux.Lock()
	_, ok := pb.prevReq[key]
	backup := pb.currView.View.Backup
	pb.mux.Unlock()
	if ok == false {
		pb.mux.Lock()
		persistantArgs := &(*args)
		persistantReply := &(*reply)
		pb.prevReq[key] = &ReqCard{GArgs: persistantArgs, GReply: persistantReply, PArgs: nil, PReply: nil, PrevReqKey: key, Processed: false}
		pb.Dlog.Printf("Case handleGet 2: Get request to server %s: init request with key %s and reqcard %+v", pb.me, key, pb.prevReq[key])
		pb.mux.Unlock()
		prevReqBackup := pb.makeReplicaPrevReq(pb.prevReq)
		if backup != "" {
			fargs := ForwardArgs{ReqType: PrevReqUpdate, Database: nil, PrevReq: prevReqBackup}
			freply := ForwardReply{Err: "", Database: nil, PrevReq: nil}
			pb.handleBackupForward(&fargs, &freply)
		}
		pb.mux.Lock()
		reqCard := pb.prevReq[key]
		pb.mux.Unlock()
		pb.mux.Lock()
		reqKey := reqCard.PrevReqKey
		dbKey := reqCard.GArgs.Key
		val, ok := pb.db[dbKey]
		if ok == true {
			pb.prevReq[reqKey].GReply.Value = val
			pb.prevReq[reqKey].GReply.Err = OK
			reply.Value = val
			reply.Err = OK
		} else {
			pb.prevReq[reqKey].GReply.Value = ""
			pb.prevReq[reqKey].GReply.Err = ErrNoKey
			reply.Value = val
			reply.Err = OK
		}
		pb.Dlog.Printf("Case handleGet 2.1: Get request finished for %s with key: %s, reply:%+v", pb.me, key, reply)
		pb.prevReq[reqKey].Processed = true
		go pb.removeOldRequest(reqKey)
		pb.mux.Unlock()
	} else {
		pb.mux.Lock()
		processed := pb.prevReq[key].Processed
		pb.mux.Unlock()
		pb.Dlog.Printf("Case handleGet 3: Put request to server %s with key %s will not be processed twice", pb.me, key)
		if processed == true {
			pb.Dlog.Printf("Case handleGet 3.1: Already processed, returning old saved response")
			pb.mux.Lock()
			reply.Value = pb.prevReq[key].GReply.Value
			reply.Err = OK
			pb.mux.Unlock()
		}
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mux.Lock()
	state := pb.state
	pb.mux.Unlock()
	if state == Reboot {
		reply.Err = ErrWrongServer
		return nil
	} else if state == Idle || state == Backup {
		pb.mux.Lock()
		currView := pb.currView
		pb.mux.Unlock()
		pb.Dlog.Println("Call made")
		call(currView.View.Primary, "PBService.Get", args, reply)
		return nil
	} else if state == Primary {
		for pb.CurrentGetRequest == true {
			time.Sleep(PingInterval)
		}
		pb.mux.Lock()
		pb.CurrentGetRequest = true
		pb.mux.Unlock()
		pb.handleGet(args, reply)
		pb.mux.Lock()
		pb.CurrentGetRequest = false
		pb.mux.Unlock()
	} else {
		errorString := fmt.Sprintf("Valid states are Reboot, Idle, Backup, and Primary. Not %s", pb.state)
		panic(errorString)
	}
	return nil
}

func (pb *PBServer) handlePut(args *PutArgs, reply *PutReply) {
	pb.mux.Lock()
	pb.Dlog.Printf("Case handlePut 1: Put request to server %s requested", pb.me)
	key := pb.buildPrevReqKey(args.Sender, args.ReqType, args.ReqNum, args.Key, args.Value)
	_, ok := pb.prevReq[key]
	backup := pb.currView.View.Backup
	pb.mux.Unlock()
	if ok == false {
		pb.mux.Lock()
		persistantArgs := &(*args)
		persistantReply := &(*reply)
		pb.prevReq[key] = &ReqCard{PArgs: persistantArgs, PReply: persistantReply, GArgs: nil, ReqType: args.ReqType, GReply: nil, PrevReqKey: key, Processed: false}
		pb.Dlog.Printf("Case handlePut 2: Put request to server %s: init request with key %s and reqcard %+v", pb.me, key, pb.prevReq[key])
		pb.mux.Unlock()
		prevReq := pb.makeReplicaPrevReq(pb.prevReq)
		if backup != "" {
			pb.mux.Lock()
			fargs := ForwardArgs{ReqType: PrevReqUpdate, Database: nil, PrevReq: prevReq}
			freply := ForwardReply{Err: "", Database: nil, PrevReq: nil}
			pb.mux.Unlock()
			pb.handleBackupForward(&fargs, &freply)
		}
		pb.mux.Lock()
		reqCard := pb.prevReq[key]
		pb.mux.Unlock()
		// reqCard.PArgs.Key, reqCard.PArgs.Value)
		pb.mux.Lock()
		dbKey := reqCard.PArgs.Key
		value := reqCard.PArgs.Value
		doHash := reqCard.PArgs.DoHash
		reqKey := reqCard.PrevReqKey
		var newValue string
		var prevValue string
		val, ok := pb.db[dbKey]
		if ok == true {
			prevValue = val
		} else {
			prevValue = ""
		}
		if doHash == true {
			newValue = strconv.Itoa(int(hash(prevValue + value)))
		} else {
			newValue = value
		}
		pb.db[dbKey] = newValue
		currView := pb.currView
		pb.mux.Unlock()
		replicaDb := pb.makeReplicaDb(pb.db)
		replicaPrevReq := pb.makeReplicaPrevReq(pb.prevReq)
		if currView.View.Backup != "" {
			pb.mux.Lock()
			pb.Dlog.Printf("Case handlePut 2.1: Server %s has backup %s", pb.me, currView.View.Backup)
			args := ForwardArgs{ReqType: DbUpdate, Database: replicaDb, PrevReq: replicaPrevReq, Key: key}
			reply := ForwardReply{Err: "", Database: nil, PrevReq: nil}
			pb.mux.Unlock()
			pb.handleBackupForward(&args, &reply)
		}
		pb.mux.Lock()
		reqCard.PReply.PreviousValue = prevValue
		reqCard.PReply.Err = OK
		pb.prevReq[reqKey].Processed = true
		reply.PreviousValue = prevValue
		reply.Err = OK
		pb.Dlog.Printf("Case handlePut 2.2: Put request finished for %s with key: %s, reply:%+v", pb.me, key, reply)
		pb.mux.Unlock()
		go pb.removeOldRequest(reqKey)
	} else {
		pb.mux.Lock()
		processed := pb.prevReq[key].Processed
		pb.mux.Unlock()
		pb.Dlog.Printf("Case handlePut 3: Put request to server %s with key %s will not be processed twice", pb.me, key)
		if processed == true {
			pb.Dlog.Printf("Case handlePut 3.1: Already processed, returning old saved response")
			pb.mux.Lock()
			reply.PreviousValue = pb.prevReq[key].PReply.PreviousValue
			reply.Err = OK
			pb.mux.Unlock()
		}
	}
}

// func (pb *PBServer) handleDeath() {
// 	for pb.dead == false {
// 		time.Sleep(time.Millisecond)
// 	}
// 	pb.mux.Lock()
// 	time.Sleep(PingInterval * 2)
// 	pb.mux.Unlock()
// }

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mux.Lock()
	pb.Dlog.Printf("Case Put 1: Put request to server %s with args %+v and reply %+v", pb.me, args, reply)
	state := pb.state
	pb.mux.Unlock()
	if state == Reboot {
		pb.Dlog.Printf("Case Put 2: Put request to server %s is in reboot state", pb.me)
		reply.Err = ErrWrongServer
		return nil
	} else if state == Idle || state == Backup {
		pb.Dlog.Printf("Case Put 3: Put request to server %s is in idle, backup.", pb.me)
		pb.mux.Lock()
		currView := pb.currView
		pb.mux.Unlock()
		pb.Dlog.Println("Call made")
		call(currView.View.Primary, "PBServer.Put", args, reply)
		return nil
	} else if state == Primary {
		pb.Dlog.Printf("Case Put 4: Put request to server %s will now handle primary request", pb.me)
		for pb.CurrentPutRequest == true {
			time.Sleep(PingInterval)
		}
		pb.mux.Lock()
		pb.CurrentPutRequest = true
		pb.mux.Unlock()
		pb.handlePut(args, reply)
		pb.mux.Lock()
		pb.CurrentPutRequest = false
		pb.mux.Unlock()
	} else {
		errorString := fmt.Sprintf("Valid states are Reboot, Idle, Backup, and Primary. Not %s", pb.state)
		panic(errorString)
	}
	return nil
}

func (pb *PBServer) isDead() bool {
	dead := false
	pb.mux.Lock()
	if pb.dead == true {
		dead = true
	}
	pb.mux.Unlock()
	return dead
}

func (pb *PBServer) notInView() bool {
	ans := false
	pb.mux.Lock()
	switch pb.me {
	case pb.currView.View.Primary:
		ans = true
	case pb.currView.View.Backup:
		ans = true
	case pb.currView.View.Idle:
		ans = true
	}
	pb.mux.Unlock()
	return ans
}

func (pb *PBServer) reprocessPendingRequests() {
	pb.Dlog.Println("Reprocessing Pending Requests")
	pb.mux.Lock()
	pb.CurrentGetRequest = true
	pb.CurrentPutRequest = true
	pb.mux.Unlock()
	for key, value := range pb.prevReq {
		if value.Processed == false {
			pb.Dlog.Println("Reprocessing req with key ", key)
			delete(pb.prevReq, key)
			if value.ReqType == Get {
				args := value.GArgs
				reply := value.GReply
				pb.handleGet(args, reply)
			} else if value.ReqType == Put {
				args := value.PArgs
				reply := value.PReply
				pb.handlePut(args, reply)
			}
		}
	}
	pb.mux.Lock()
	pb.CurrentGetRequest = false
	pb.CurrentPutRequest = false
	pb.mux.Unlock()
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	pb.mux.Lock()
	pb.Dlog.Println("Tick Case 0: Launching ticker for ", pb.me, " in ", pb.state)
	pb.mux.Unlock()
	// if pb.isDead() == true {
	// 	pb.Dlog.Printf("Tick Case 0.1: Server %s attempted tick while dead", pb.me)
	// 	return
	// }
	view, _ := pb.vs.Get()
	pb.mux.Lock()
	state := pb.state
	currView := pb.currView
	pb.mux.Unlock()
	if state != Reboot && currView.View.Viewnum == 0 {
		pb.Dlog.Printf("Viewnum is 0 and state is %s. Rebooting.", state)
		pb.resetServer()
		return
	}
	if state == Reboot {
		pb.Dlog.Println("Tick Case 1: Reboot server for ", pb.me, " in ", pb.state)
		var task State
		task = ""
		switch pb.me {
		case view.Primary:
			task = Primary
		case view.Backup:
			task = Backup
			go pb.initBackupAsMe()
		case view.Idle:
			task = Idle
		}
		if task != "" {
			pb.mux.Lock()
			pb.state = task
			v := View{Primary: view.Primary, Backup: view.Backup, Idle: view.Idle, Viewnum: view.Viewnum}
			vc := ViewCard{View: &v, prevPing: time.Now()}
			pb.currView = &vc
			pb.Dlog.Printf("Tick Case 1.1: Finished Init for %s with value %+v with state %s", pb.me, pb.currView, pb.state)
			pb.mux.Unlock()
		} else {
			pb.Dlog.Printf(" TickCase 1.2: Task not initialized. Server %s not setup just yet. Pinging 0 to register.", pb.me)
			pb.vs.Ping(0)
			return
		}
	} else {
		pb.Dlog.Println("View: ", currView.View.Primary, currView.View.Backup, currView.View.Idle, currView.View.Viewnum)
		pb.Dlog.Printf("Tick Case 2: Server %s tick with value %+v and with state %s", pb.me, pb.currView, pb.state)
		if currView.View.Viewnum != view.Viewnum {
			pb.Dlog.Printf("Tick Case 2.1: Attempt to establish new view, Old: %+v vs %+v", currView.View, view)
			if state == Backup && pb.me == view.Primary {
				pb.Dlog.Printf("Tick Case 2.1.1: Upgrading backup to primary")
				pb.reprocessPendingRequests()
				pb.mux.Lock()
				pb.state = Primary
				pb.mux.Unlock()
			}
			if pb.state == Idle && pb.me == view.Backup {
				pb.Dlog.Printf("Tick Case 2.1.2: Upgrading idle to backup")
				go pb.initBackupAsMe()
				pb.mux.Lock()
				pb.state = Backup
				pb.mux.Unlock()
			}
		}
		pb.mux.Lock()
		pb.currView.View.Viewnum = view.Viewnum
		pb.currView.View.Primary = view.Primary
		pb.currView.View.Backup = view.Backup
		pb.currView.View.Idle = view.Idle
		pb.currView.prevPing = time.Now()
		pb.Dlog.Printf("Tick Case 2.2: Updating all values for state: %+v", pb.currView)
		pb.mux.Unlock()
	}
	// exp := pb.viewExpired()
	pb.mux.Lock()
	state = pb.state
	currView = pb.currView
	pb.mux.Unlock()
	if state != Reboot /*&& exp == false*/ {
		if pb.isDead() == false {
			pb.vs.Ping(currView.View.Viewnum)
		}
	}

	//else {
	//pb.resetServer()
	//}
}

/*else if exp == true {
	fmt.Println("Here")
	pb.resetServer()
	// if pb.isDead() == false {
	// pb.vs.Ping(0)
	// }
}*/

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.mux.Lock()
	pb.dead = true
	pb.mux.Unlock()
	pb.l.Close()
}

func (pb *PBServer) resetMaps() {
	pb.mux.Lock()
	for key, _ := range pb.db {
		delete(pb.db, key)
	}
	for key, _ := range pb.prevReq {
		delete(pb.prevReq, key)
	}
	pb.mux.Unlock()
}

func (pb *PBServer) resetView() {
	v := View{Primary: "", Backup: "", Idle: "", Viewnum: 0}
	vc := ViewCard{View: &v, prevPing: time.Now()}
	pb.currView = &vc
	pb.state = Reboot
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	pb.resetView()
	pb.db = make(map[string]string)
	pb.prevReq = make(map[string]*ReqCard)
	// Your pb.* initializations here.
	f := pb.createDebugger(pb.me)
	pb.Dlog.Println("Step 1: Starting pbserver")
	go func(file *os.File) {
		for pb.dead == false {
			time.Sleep(PingInterval)
		}
		f.Close()
	}(f)
	// go pb.handleDeath()
	pb.resetServer()
	// go pb.reqHandler()
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
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.resetServer()
		pb.done.Done()
	}()

	return pb
}
