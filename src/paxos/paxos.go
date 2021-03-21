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

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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

type AcceptorState struct {
	sync.Mutex
	HighestPrepare     int
	HighestAccept      int
	HighestAcceptValue interface{}
}

type ProposerState struct {
	sync.Mutex
	Chosen        interface{}
	HighestAccept int
}

type Log struct {
	Seq     int
	Value   interface{}
	Decided bool
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
	logs      []Log
	peersDone []int
	acceptors map[int]*AcceptorState
}

type PrepareArgs struct {
	Seq int
	N   int
	V   interface{}
}

type PrepareReply struct {
	Success bool
	Seq     int
	N       int
	N_a     int
	V_a     interface{}
}

func (px *Paxos) getLogBySeq(seq int) (*Log, int) {
	l, r := 0, len(px.logs)
	for l < r {
		mid := (r-l)/2 + l
		if px.logs[mid].Seq == seq {
			return &px.logs[mid], mid
		}
		if px.logs[mid].Seq < seq {
			l = mid + 1
		} else {
			r = mid
		}
	}
	return nil, -1
}

func (px *Paxos) expandLogsTo(to int) {
	if to < px.peersDone[px.me] {
		return
	}
	from := px.peersDone[px.me] + 1
	if len(px.logs) > 0 {
		from = px.logs[len(px.logs)-1].Seq + 1
	}
	for i := from; i <= to; i++ {
		px.logs = append(px.logs, Log{
			Seq: i,
		})
	}
	return
}

func (px *Paxos) getAcceptorState(seq int) (state *AcceptorState) {
	px.mu.Lock()
	defer px.mu.Unlock()
	state = px.acceptors[seq]
	if state == nil {
		state = &AcceptorState{
			HighestPrepare: -1,
			HighestAccept:  -1,
		}
		px.acceptors[seq] = state
	}
	return
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	state := px.getAcceptorState(args.Seq)
	state.Lock()
	defer state.Unlock()
	DPrintf("Peer %v Received Prepare Seq %v N %v, Acceptor State %#v\n", px.me, args.Seq, args.N, state)
	if args.N > state.HighestPrepare {
		state.HighestPrepare = args.N
		reply.Success = true
		reply.N = args.N
		reply.N_a = state.HighestAccept
		reply.V_a = state.HighestAcceptValue
		return nil
	}

	reply.Success = false
	return nil
}

type AcceptArgs struct {
	Seq int
	N   int
	V   interface{}
}

type AcceptReply struct {
	Success bool
	Seq     int
	N       int

	DoneSeq int // piggyback
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	state := px.getAcceptorState(args.Seq)
	state.Lock()
	defer state.Unlock()
	DPrintf("Peer %v Received Accept Seq %v N %v, Acceptor State %#v\n", px.me, args.Seq, args.N, state)
	if args.N >= state.HighestPrepare {
		state.HighestPrepare = args.N
		state.HighestAccept = args.N
		state.HighestAcceptValue = args.V

		reply.Success = true
		reply.N = args.N
		reply.Seq = args.Seq
		return nil
	}
	reply.Success = false
	return nil
}

func (px *Paxos) proposerDecided(seq int, v interface{}) {
	DPrintf("Peer %v send Decided Seq %v\n", px.me, seq)
	args := DecidedArgs{
		Seq: seq,
		V:   v,
	}
	for idx, srv := range px.peers {
		if idx == px.me {
			reply := DecidedReply{}
			go px.Decided(&args, &reply)
		} else {
			go func(srv string) {
				reply := DecidedReply{}
				call(srv, "Paxos.Decided", &args, &reply)
			}(srv)
		}
	}
}

func (px *Paxos) proposerAccept(seq, n int, v interface{}) {
	DPrintf("Peer %v started Accept Seq %v N %v\n", px.me, seq, n)
	majority := len(px.peers)/2 + 1
	acceptCount := uint32(0)
	args := AcceptArgs{
		Seq: seq,
		N:   n,
		V:   v,
	}

	onAcceptReply := func(reply *AcceptReply) {
		DPrintf("Peer %v got Accept Reply %#v\n", px.me, reply)
		if !reply.Success {
			return
		}
		accepted := atomic.AddUint32(&acceptCount, 1)
		if accepted >= uint32(majority) {
			px.proposerDecided(seq, v)
		}
	}
	for idx, srv := range px.peers {
		if idx == px.me {
			reply := AcceptReply{}
			px.Accept(&args, &reply)
			onAcceptReply(&reply)
		} else {
			go func(srv string) {
				reply := AcceptReply{}
				ok := call(srv, "Paxos.Accept", &args, &reply)
				if ok {
					onAcceptReply(&reply)
				}
			}(srv)
		}
	}
}

func (px *Paxos) proposerPropose(seq int, v interface{}) {
	n := px.me
	majority := len(px.peers)/2 + 1
	state := ProposerState{Chosen: v}
	prepareCount := uint32(0)
	repliedCount := uint32(0)
	acceptStated := uint32(0)
	// for !px.isdead() {
	DPrintf("Peer %v Propose Seq %v N %v\n", px.me, seq, n)
	args := PrepareArgs{
		Seq: seq,
		N:   n,
		V:   v,
	}

	onPrepareReply := func(reply *PrepareReply) {
		DPrintf("Peer %v Got Prepare Reply %#v\n", px.me, reply)
		atomic.AddUint32(&repliedCount, 1)
		if !reply.Success {
			return
		}
		prepared := atomic.AddUint32(&prepareCount, 1)
		if prepared >= uint32(majority) {
			state.Lock()
			if reply.N_a > state.HighestAccept {
				state.Chosen = reply.V_a
			}
			if atomic.CompareAndSwapUint32(&acceptStated, 0, 1) {
				go px.proposerAccept(seq, n, state.Chosen)
			}
			state.Unlock()
		}
	}
	for idx, srv := range px.peers {
		if idx == px.me {
			reply := PrepareReply{}
			go func() {
				px.Prepare(&args, &reply)
				onPrepareReply(&reply)
			}()
		} else {
			go func(srv string) {
				reply := PrepareReply{}
				ok := call(srv, "Paxos.Prepare", &args, &reply)
				if ok {
					onPrepareReply(&reply)
				}
			}(srv)
		}
	}
	// n += len(px.peers)
	// }
}

type DecidedArgs struct {
	Seq int
	V   interface{}
}

type DecidedReply struct {
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.expandLogsTo(args.Seq)
	log, _ := px.getLogBySeq(args.Seq)
	log.Decided = true
	log.Value = args.V
	return nil
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

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.mu.Lock()
	px.expandLogsTo(seq)
	px.mu.Unlock()
	go px.proposerPropose(seq, v)
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
	if seq < px.peersDone[px.me] {
		return
	}
	px.peersDone[px.me] = seq
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
	if px.peersDone[px.me] == -1 && len(px.logs) == 0 {
		return 0
	}
	return px.peersDone[px.me] + len(px.logs)
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
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	m := px.peersDone[px.me]
	for _, d := range px.peersDone {
		if d < m {
			m = d
		}
	}
	return m + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if len(px.logs) == 0 || seq < px.peersDone[px.me] {
		return Forgotten, nil
	}
	px.expandLogsTo(seq)
	log, _ := px.getLogBySeq(seq)
	// DPrintf("Peer %v %#v %#v\n", px.me, log, px.logs)
	status := Pending
	if log.Decided {
		status = Decided
	}
	return status, log.Value
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
	px.peersDone = make([]int, len(peers))
	px.acceptors = make(map[int]*AcceptorState)
	for idx := range px.peersDone {
		px.peersDone[idx] = -1
	}

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
