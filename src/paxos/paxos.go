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
	"time"
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
	HighestPrepare     int
	HighestAccept      int
	HighestAcceptValue interface{}
}

type ProposerState struct {
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
	logs      map[int]interface{}
	peersDone []int
	acceptors map[int]*AcceptorState
	maxSeq    int
}

type PrepareResult struct {
	Success bool
	Value   interface{}
}

type PrepareArgs struct {
	Seq      int
	Proposal int
	Value    interface{}

	Id   int
	Done int
}

type PrepareReply struct {
	Success          bool
	Seq              int
	Proposal         int
	ProposalAccepted int
	ValueAccepted    interface{}

	Done int
}

func (px *Paxos) Lock() {
	// DPrintf("Peer %v Lock\n", px.me)
	px.mu.Lock()
}

func (px *Paxos) Unlock() {
	// DPrintf("Peer %v Unlock\n", px.me)
	px.mu.Unlock()
}

func (px *Paxos) getAcceptorState(seq int) (state *AcceptorState) {
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

func (px *Paxos) updateMaxSeq(seq int) {
	if seq > px.maxSeq {
		px.maxSeq = seq
	}
}

func (px *Paxos) Prepare(args PrepareArgs, reply *PrepareReply) error {
	DPrintf("Peer %v Received Prepare From %v Seq %v N %v\n", px.me, args.Id, args.Seq, args.Proposal)

	px.Lock()
	defer px.Unlock()

	px.updatePeerDone(args.Id, args.Done)
	state := px.getAcceptorState(args.Seq)

	reply.Done = px.peersDone[px.me]
	reply.Seq = args.Seq
	if args.Seq <= reply.Done {
		return nil
	}
	px.updateMaxSeq(args.Seq)

	if args.Proposal > state.HighestPrepare {
		state.HighestPrepare = args.Proposal

		reply.Success = true
		reply.Proposal = args.Proposal
		reply.ProposalAccepted = state.HighestAccept
		reply.ValueAccepted = state.HighestAcceptValue
		return nil
	}

	reply.Success = false
	reply.ProposalAccepted = state.HighestPrepare
	return nil
}

type AcceptArgs struct {
	Seq int
	N   int
	V   interface{}

	Id   int
	Done int
}

type AcceptReply struct {
	Success bool
	Seq     int
	N       int

	Done int // piggyback
}

func (px *Paxos) Accept(args AcceptArgs, reply *AcceptReply) error {
	DPrintf("Peer %v Received Accept From %v Seq %v N %v\n", px.me, args.Id, args.Seq, args.N)
	// defer DPrintf("Peer %v finished Seq %v Accept\n", px.me, args.Seq)
	px.Lock()
	defer px.Unlock()

	px.updatePeerDone(args.Id, args.Done)
	state := px.getAcceptorState(args.Seq)

	reply.Done = px.peersDone[px.me]
	reply.Success = false
	if args.Seq <= reply.Done {
		return nil
	}
	px.updateMaxSeq(args.Seq)

	if args.N >= state.HighestPrepare {
		state.HighestPrepare = args.N
		state.HighestAccept = args.N
		state.HighestAcceptValue = args.V

		reply.Success = true
		reply.N = args.N
		reply.Seq = args.Seq
		return nil
	}

	return nil
}

func (px *Paxos) sendDecidedToAll(seq int, v interface{}) {
	DPrintf("Peer %v send Decided Seq %v\n", px.me, seq)

	px.Lock()
	args := DecidedArgs{
		Seq: seq,
		V:   v,

		Id:   px.me,
		Done: px.peersDone[px.me],
	}
	px.Unlock()

	for idx, srv := range px.peers {
		if idx == px.me {
			reply := DecidedReply{}
			px.Decided(args, &reply)
		} else {
			reply := DecidedReply{}
			call(srv, "Paxos.Decided", &args, &reply)
		}
	}
}

func (px *Paxos) sendAcceptToAll(seq, n int, v interface{}) bool {
	DPrintf("Peer %v started Accept Seq %v N %v\n", px.me, seq, n)
	majority := len(px.peers)/2 + 1
	acceptCount := 0

	px.Lock()
	args := AcceptArgs{
		Seq: seq,
		N:   n,
		V:   v,

		Id:   px.me,
		Done: px.peersDone[px.me],
	}
	px.Unlock()

	for idx, srv := range px.peers {
		var ok bool
		reply := AcceptReply{}
		if idx == px.me {
			px.Accept(args, &reply)
			ok = true
		} else {
			ok = call(srv, "Paxos.Accept", &args, &reply)
		}

		if !ok {
			continue
		}
		DPrintf("Peer %v got Accept reply from %v: %#v\n", px.me, idx, reply)

		px.Lock()
		px.updatePeerDone(idx, reply.Done)
		px.Unlock()

		if !reply.Success {
			continue
		}
		acceptCount++
	}

	return acceptCount >= majority
}

func (px *Paxos) truncateLogsUntil(idx int) {
	for seq := range px.logs {
		if seq <= idx {
			delete(px.logs, seq)
			delete(px.acceptors, seq)
		}
	}
}

func (px *Paxos) updatePeerDone(peer, done int) {
	if px.peersDone[peer] < done {
		px.peersDone[peer] = done
		m := px.minNoLock()
		px.truncateLogsUntil(m - 1)
	}
}

func (px *Paxos) sendPrepareToAll(seq, n int, v interface{}) PrepareResult {
	majority := len(px.peers)/2 + 1
	state := ProposerState{Chosen: v, HighestAccept: -1}
	prepareCount := 0

	px.Lock()
	args := PrepareArgs{
		Seq:      seq,
		Proposal: n,
		Value:    v,

		Id:   px.me,
		Done: px.peersDone[px.me],
	}
	px.Unlock()

	for idx, srv := range px.peers {
		var ok bool
		reply := PrepareReply{}
		if idx == px.me {
			px.Prepare(args, &reply)
			ok = true
		} else {
			ok = call(srv, "Paxos.Prepare", &args, &reply)
		}

		if !ok {
			continue
		}

		DPrintf("Peer %v Got Prepare Reply From %v: Success %v ProposalAccepted %v \n", px.me, idx, reply.Success, reply.ProposalAccepted)

		px.Lock()
		px.updatePeerDone(idx, reply.Done)
		px.Unlock()

		if !reply.Success {
			px.Lock()
			acceptorState := px.getAcceptorState(seq)
			if reply.ProposalAccepted > acceptorState.HighestPrepare {
				acceptorState.HighestPrepare = reply.ProposalAccepted
				px.Unlock()
				return PrepareResult{false, nil}
			}
			px.Unlock()
			continue
		}
		prepareCount++
		if reply.ProposalAccepted > state.HighestAccept {
			state.Chosen = reply.ValueAccepted
			state.HighestAccept = reply.ProposalAccepted
		}
	}

	if prepareCount >= majority {
		return PrepareResult{true, state.Chosen}
	}

	return PrepareResult{false, nil}
}

func (px *Paxos) proposer(seq int, v interface{}) {
	for n := px.me; !px.isdead(); n += len(px.peers) {
		px.Lock()
		if n <= px.getAcceptorState(seq).HighestPrepare {
			px.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if seq <= px.peersDone[px.me] {
			px.Unlock()
			break
		}
		if _, ok := px.logs[seq]; ok {
			DPrintf("Peer %v Seq %v Decided, exit proposer\n", px.me, seq)
			px.Unlock()
			break
		}
		px.Unlock()

		DPrintf("Peer %v Propose Seq %v N %v\n", px.me, seq, n)
		prepareResult := px.sendPrepareToAll(seq, n, v)
		if !prepareResult.Success {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		v = prepareResult.Value

		accepted := px.sendAcceptToAll(seq, n, prepareResult.Value)
		if !accepted {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		px.sendDecidedToAll(seq, prepareResult.Value)
		break
	}
	DPrintf("Peer %v Seq %v Proposer exited\n", px.me, seq)
}

type DecidedArgs struct {
	Seq int
	V   interface{}

	Id   int
	Done int
}

type DecidedReply struct {
	Done int
}

func (px *Paxos) Decided(args DecidedArgs, reply *DecidedReply) error {
	DPrintf("Peer %v Received Decided from %v Seq %v\n", px.me, args.Id, args.Seq)
	px.Lock()
	defer px.Unlock()

	px.updatePeerDone(args.Id, args.Done)
	reply.Done = px.peersDone[px.me]
	if args.Seq <= reply.Done {
		return nil
	}
	px.updateMaxSeq(args.Seq)
	px.logs[args.Seq] = args.V
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
			// fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
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
	px.Lock()
	px.updateMaxSeq(seq)
	px.Unlock()

	go px.proposer(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.Lock()
	defer px.Unlock()
	DPrintf("Peer %v Done Seq %v\n", px.me, seq)
	px.updatePeerDone(px.me, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.Lock()
	defer px.Unlock()
	return px.maxSeq
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
	px.Lock()
	defer px.Unlock()
	return px.minNoLock()
}

func (px *Paxos) minNoLock() int {
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
	px.Lock()
	defer px.Unlock()
	if len(px.logs) == 0 || seq < px.minNoLock() {
		return Forgotten, nil
	}

	px.updateMaxSeq(seq)
	v, ok := px.logs[seq]
	if ok {
		return Decided, v
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
	px.logs = make(map[int]interface{})
	px.peersDone = make([]int, len(peers))
	px.acceptors = make(map[int]*AcceptorState)
	px.maxSeq = -1
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
