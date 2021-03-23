package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpCode int

const (
	Unknown OpCode = iota
	Get
	Put
	Append
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	RequestID uint64
	Op        OpCode
	Key       string
	Value     string
}

type cachedReply struct {
	Err   string
	Value string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	applySeq      int
	nextSeq       int
	cachedReplies map[int64]map[uint64]cachedReply
	store         map[string]string
}

func (kv *KVPaxos) doOp(op Op) {
	if _, ok := kv.cachedReplies[op.ClientID][op.RequestID]; ok {
		return
	}

	cached := cachedReply{Err: OK}
	switch op.Op {
	case Get:
		cached.Value = kv.store[op.Key]
	case Put:
		kv.store[op.Key] = op.Value
	case Append:
		kv.store[op.Key] += op.Value
	}
	kv.cacheOneReply(op.ClientID, op.RequestID, cached)
}

func (kv *KVPaxos) applyLogs(to int) {
	for seq := kv.applySeq + 1; seq <= to; seq++ {
		if status, value := kv.px.Status(seq); status == paxos.Decided {
			op := value.(Op)
			kv.doOp(op)
			kv.applySeq = seq
			continue
		}
		break
	}
}

func (kv *KVPaxos) getNextSeq() int {
	seq := kv.nextSeq
	kv.nextSeq++
	return seq
}

func isSameRequest(op1, op2 Op) bool {
	return op1.ClientID == op2.ClientID && op1.RequestID == op2.RequestID
}

func (kv *KVPaxos) cacheOneReply(clientID int64, requestID uint64, cached cachedReply) {
	if kv.cachedReplies[clientID] == nil {
		kv.cachedReplies[clientID] = make(map[uint64]cachedReply)
	}
	kv.cachedReplies[clientID][requestID] = cached
}

func (kv *KVPaxos) deleteOneCache(clientID int64, requestID uint64) {
	delete(kv.cachedReplies[clientID], requestID)
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	DPrintf("KV Server %v received Get request ClientID %v RequestID %v\n", kv.me, args.ClientID, args.RequestID)
	kv.mu.Lock()
	kv.deleteOneCache(args.ClientID, args.LastRequestID)

	if cached, ok := kv.cachedReplies[args.ClientID][args.RequestID]; ok {
		reply.Err = Err(cached.Err)
		reply.Value = cached.Value
		kv.mu.Unlock()
		return nil
	}

	seq := kv.px.Max() + 1
	kv.mu.Unlock()

	op := Op{
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		Op:        Get,
		Key:       args.Key,
	}

	DPrintf("KV Server %v assigned Get request ClientID %v RequestID %v Seq %v\n", kv.me, args.ClientID, args.RequestID, seq)
	kv.px.Start(seq, op)

	to := 10 * time.Millisecond
	for {
		status, value := kv.px.Status(seq)
		DPrintf("KV Server %v check Seq %v Status %v\n", kv.me, seq, status)
		if status == paxos.Decided {
			agreedOp := value.(Op)
			if !isSameRequest(op, agreedOp) {
				kv.mu.Lock()
				seq = kv.px.Max() + 1
				kv.mu.Unlock()
				DPrintf("KV Server %v assigned Get request ClientID %v RequestID %v Seq %v\n", kv.me, args.ClientID, args.RequestID, seq)
				kv.px.Start(seq, op)
				to = 10 * time.Millisecond
				continue
			} else {
				kv.mu.Lock()
				for kv.applySeq < seq {
					kv.applyLogs(seq)
					kv.mu.Unlock()
					time.Sleep(10 * time.Millisecond)
					kv.mu.Lock()
				}
				cached := kv.cachedReplies[args.ClientID][args.RequestID]
				reply.Err = Err(cached.Err)
				reply.Value = cached.Value
				kv.px.Done(kv.applySeq)
				kv.mu.Unlock()
				return nil
			}
		} else if status == paxos.Forgotten {
			kv.mu.Lock()
			cached := kv.cachedReplies[args.ClientID][args.RequestID]
			reply.Err = Err(cached.Err)
			reply.Value = cached.Value
			kv.px.Done(kv.applySeq)
			kv.mu.Unlock()

			return nil
		}
		time.Sleep(to)
		if to < 1*time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	DPrintf("KV Server %v received PutAppend request ClientID %v RequestID %v\n", kv.me, args.ClientID, args.RequestID)
	kv.mu.Lock()

	kv.deleteOneCache(args.ClientID, args.LastRequestID)

	if cached, ok := kv.cachedReplies[args.ClientID][args.RequestID]; ok {
		reply.Err = Err(cached.Err)
		kv.mu.Unlock()
		return nil
	}

	seq := kv.px.Max() + 1
	kv.mu.Unlock()

	opCode := Put
	if args.Op == "Append" {
		opCode = Append
	}

	op := Op{
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		Op:        opCode,
		Key:       args.Key,
		Value:     args.Value,
	}
	DPrintf("KV Server %v assigned PutAppend request ClientID %v RequestID %v Seq %v\n", kv.me, args.ClientID, args.RequestID, seq)
	kv.px.Start(seq, op)

	to := 10 * time.Millisecond
	for {
		status, value := kv.px.Status(seq)
		DPrintf("KV Server %v check Seq %v Status %v\n", kv.me, seq, status)
		if status == paxos.Decided {
			agreedOp := value.(Op)
			if !isSameRequest(op, agreedOp) {
				kv.mu.Lock()
				seq = kv.px.Max() + 1
				kv.mu.Unlock()
				DPrintf("KV Server %v assigned PutAppend request ClientID %v RequestID %v Seq %v\n", kv.me, args.ClientID, args.RequestID, seq)
				kv.px.Start(seq, op)
				to = 10 * time.Millisecond
				continue
			} else {
				kv.mu.Lock()
				for kv.applySeq < seq {
					kv.applyLogs(seq)
					kv.mu.Unlock()
					time.Sleep(10 * time.Millisecond)
					kv.mu.Lock()
				}
				cached := kv.cachedReplies[args.ClientID][args.RequestID]
				reply.Err = Err(cached.Err)
				kv.px.Done(kv.applySeq)
				kv.mu.Unlock()
				return nil
			}
		} else if status == paxos.Forgotten {
			kv.mu.Lock()
			cached := kv.cachedReplies[args.ClientID][args.RequestID]
			reply.Err = Err(cached.Err)
			kv.px.Done(kv.applySeq)
			kv.mu.Unlock()
			return nil
		}
		time.Sleep(to)
		if to < 1*time.Second {
			to *= 2
		}
	}
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
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.cachedReplies = make(map[int64]map[uint64]cachedReply)
	kv.applySeq = -1
	kv.nextSeq = 0
	kv.store = make(map[string]string)

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
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
