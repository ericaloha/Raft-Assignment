package main

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"google.golang.org/grpc"
	"cuhk/asgn/raft"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
)

const DEBUG = false

func printf(format string, a ...interface{}) (n int, err error) {
	if DEBUG {
		log.Printf(format, a...)
	}
	return
}

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {
	// Generic info
	peers    map[int32]raft.RaftNodeClient
	myId     int32
	role     raft.Role
	mu       sync.Mutex
	leaderId int32

	// handle timer
	resetCurElectionTicker  chan bool
	stopCurElectionTicker   chan bool
	resetCurHeartBeatTicker chan bool
	stopCurHeartBeatTicker  chan bool
	heartBeatInterval       int

	// Persistent state on all servers
	currentTerm int32
	votedFor    int32
	log         []*raft.LogEntry

	// kv store
	kvStore map[string]int32

	// Volatile state on all servers
	commitIndex int32

	// Volatile state on leaders
	nextIndex  []int32
	matchIndex []int32

	// for leader
	notifyHearBeat chan bool
	waitingOp      map[int32]chan bool
	// for candidate
	stopCurElection chan bool
}

func (rn *raftNode) getRole() raft.Role {
	rn.mu.Lock()
	role := rn.role
	rn.mu.Unlock()
	return role
}

func (rn *raftNode) setRole(role raft.Role) {
	rn.mu.Lock()
	rn.role = role
	rn.mu.Unlock()
}

func (rn *raftNode) getCurrentTerm() int32 {
	return atomic.LoadInt32(&rn.currentTerm)
}

func (rn *raftNode) setCurrentTerm(term int32) {
	atomic.StoreInt32(&rn.currentTerm, term)
}

func (rn *raftNode) getLastLogIndex() int32 {
	rn.mu.Lock()
	lastLogIndex := int32(len(rn.log) - 1)
	rn.mu.Unlock()
	return lastLogIndex
}

func (rn *raftNode) getLastLogTerm() int32 {
	rn.mu.Lock()
	lastLogTerm := rn.log[len(rn.log)-1].Term
	rn.mu.Unlock()
	return lastLogTerm
}

func (rn *raftNode) getLastEntry() (int32, int32) {
	rn.mu.Lock()
	lastLogIndex := int32(len(rn.log) - 1)
	lastLogTerm := rn.log[len(rn.log)-1].Term
	rn.mu.Unlock()
	return lastLogIndex, lastLogTerm
}

func (rn *raftNode) getCommitIndedx() int32 {
	return atomic.LoadInt32(&rn.commitIndex)
}

func (rn *raftNode) setCommitIndex(commitId int32) {
	atomic.StoreInt32(&rn.commitIndex, commitId)
}

func (rn *raftNode) getVotedFor() int32 {
	return atomic.LoadInt32(&rn.votedFor)
}

func (rn *raftNode) setVotedFor(candidateId int32) {
	atomic.StoreInt32(&rn.votedFor, candidateId)
}

// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.
func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {
	// TODO: Implement this!

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		peers:                   hostConnectionMap,
		myId:                    int32(nodeId),
		role:                    raft.Role_Follower,
		leaderId:                -1,
		resetCurElectionTicker:  make(chan bool),
		stopCurElectionTicker:   make(chan bool),
		resetCurHeartBeatTicker: make(chan bool),
		stopCurHeartBeatTicker:  make(chan bool),
		currentTerm:             0,
		votedFor:                -1,
		kvStore:                 make(map[string]int32),
		commitIndex:             0,
		nextIndex:               make([]int32, len(nodeidPortMap)+1),
		matchIndex:              make([]int32, len(nodeidPortMap)+1),
		stopCurElection:         make(chan bool),
		waitingOp:               make(map[int32]chan bool),
	}
	rn.log = append(rn.log, &raft.LogEntry{Term: 0})
	for i := range rn.nextIndex {
		rn.nextIndex[i] = rn.getLastLogIndex() + 1
		rn.matchIndex[i] = 0
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect nodes
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("[%d]: Successfully connect all nodes", myport)

	//TODO: kick off leader election here !
	go rn.ElectionTicker(electionTimeout)
	go rn.HeartBeatTicker(heartBeatInterval)
	go rn.run()

	return &rn, nil
}

func (rn *raftNode) run() {
	for {
		role := rn.getRole()
		switch role {
		case raft.Role_Follower:
			rn.HandleFollower()
		case raft.Role_Candidate:
			rn.HandleCandidate()
		case raft.Role_Leader:
			rn.HandleLeader()
		}
	}
}

func (rn *raftNode) HandleFollower() {
	// empty
}

func (rn *raftNode) HandleCandidate() {
	// empty
}

func (rn *raftNode) HandleLeader() {
	rn.leaderId = rn.myId
	for index := range rn.nextIndex {
		rn.nextIndex[index] = rn.getLastLogIndex() + 1
		rn.matchIndex[index] = 0
	}

	quorumSize := (len(rn.peers)+1)/2 + 1

	respCh := make(chan *raft.AppendEntriesReply, len(rn.peers))
	rn.notifyHearBeat = make(chan bool, 1)
	rn.resetCurHeartBeatTicker <- true
	rn.notifyHearBeat <- true
	for rn.getRole() == raft.Role_Leader {
		select {
		case <-rn.notifyHearBeat:
			go rn.sendHeartBeat(respCh)
		case reply := <-respCh:
			if reply.Term > rn.getCurrentTerm() {
				rn.setRole(raft.Role_Follower)
				rn.setCurrentTerm(reply.Term)
				rn.setVotedFor(-1)
				return
			}

			rn.matchIndex[reply.From] = reply.MatchIndex
			if reply.Success {
				rn.nextIndex[reply.From] = reply.MatchIndex + 1
			} else {
				rn.nextIndex[reply.From] = rn.nextIndex[reply.From] - 1
			}

			commitId := rn.getCommitIndedx()
			nCommited := 1
			nextCommitId := rn.getLastLogIndex()
			for _, matchId := range rn.matchIndex {
				if matchId > commitId {
					nCommited += 1
					if matchId < nextCommitId {
						nextCommitId = matchId
					}
				}
			}
			if nextCommitId > commitId && nCommited >= quorumSize && rn.log[nextCommitId].Term == rn.getCurrentTerm() {
				startCommitId := rn.getCommitIndedx()
				rn.setCommitIndex(nextCommitId)
				endCommitId := rn.getCommitIndedx()
				for i := startCommitId + 1; i <= endCommitId; i++ {
					status := false
					logEntry := rn.log[i]
					if logEntry.Op == raft.Operation_Put {
						rn.kvStore[logEntry.Key] = logEntry.Value
						status = true
					} else {
						_, status = rn.kvStore[logEntry.Key]
						delete(rn.kvStore, logEntry.Key)
					}
					_, ok := rn.waitingOp[i]
					if ok {
						rn.waitingOp[i] <- status
						delete(rn.waitingOp, i)
					}
				}
			}
		}
	}
}

func (rn *raftNode) StartLeaderElection() {
	// response channel
	respCh := make(chan *raft.RequestVoteReply, len(rn.peers))

	rn.setCurrentTerm(rn.getCurrentTerm() + 1)
	currentTerm := rn.getCurrentTerm()
	lastLogIndex, lastLogTerm := rn.getLastEntry()

	for nodeId, peer := range rn.peers {

		go func(nodeId int32, client raft.RaftNodeClient) {
			request := raft.RequestVoteArgs{
				From:         rn.myId,
				To:           nodeId,
				Term:         currentTerm,
				CandidateId:  rn.myId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, _ := client.RequestVote(context.Background(), &request)
			respCh <- reply
		}(nodeId, peer)
	}

	quorumSize := (len(rn.peers)+1)/2 + 1
	var grantedVotes int32 = 0
	// self vote
	rn.setVotedFor(rn.myId)
	atomic.AddInt32(&grantedVotes, 1)

	for rn.getRole() == raft.Role_Candidate {
		select {
		case reply := <-respCh:
			if reply.Term > rn.getCurrentTerm() {
				rn.setRole(raft.Role_Follower)
				rn.setCurrentTerm(reply.Term)
				rn.setVotedFor(-1)
				return
			}

			if reply.VoteGranted {
				atomic.AddInt32(&grantedVotes, 1)
				if int(grantedVotes) >= quorumSize {
					rn.setRole(raft.Role_Leader)
					return
				}
			}
		case <-rn.stopCurElection:
			return
		}
	}
}

func (rn *raftNode) sendHeartBeat(respCh chan *raft.AppendEntriesReply) {
	currentTerm := rn.getCurrentTerm()
	commitIndex := rn.getCommitIndedx()

	for nodeId, peer := range rn.peers {

		go func(nodeId int32, client raft.RaftNodeClient) {
			prevLogEntry := rn.log[rn.nextIndex[nodeId]-1]
			EntryArgs := raft.AppendEntriesArgs{
				From:         rn.myId,
				To:           nodeId,
				Term:         currentTerm,
				LeaderId:     rn.myId,
				PrevLogIndex: rn.nextIndex[nodeId] - 1,
				PrevLogTerm:  prevLogEntry.Term,
				Entries:      rn.log[rn.nextIndex[nodeId]:],
				LeaderCommit: commitIndex,
			}
			printf("[%d] send heart beat: %v, log: %v, next: %v", rn.myId, EntryArgs, rn.log, rn.nextIndex)
			reply, _ := client.AppendEntries(context.Background(), &EntryArgs)
			printf("[%d] get heart beat reply: %v, next: %d, match: %d", rn.myId, *reply, rn.nextIndex[nodeId], rn.matchIndex[nodeId])
			respCh <- reply
		}(nodeId, peer)
	}
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	// TODO: Implement this!
	log.Printf("[%d] Receive propose from client: %v", rn.myId, *args)
	var ret raft.ProposeReply
	ret.CurrentLeader = rn.leaderId
	if rn.getRole() != raft.Role_Leader {
		ret.Status = raft.Status_WrongNode
	} else {
		logEntry := raft.LogEntry{
			Term:  rn.getCurrentTerm(),
			Op:    args.Op,
			Key:   args.Key,
			Value: args.V,
		}

		rn.log = append(rn.log, &logEntry)
		waitId := rn.getLastLogIndex()
		rn.waitingOp[waitId] = make(chan bool)
		select {
		case status := <-rn.waitingOp[waitId]:
			if status {
				ret.Status = raft.Status_OK
			} else {
				ret.Status = raft.Status_KeyNotFound
			}
		}
	}

	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	// TODO: Implement this!
	var ret raft.GetValueReply
	v, success := rn.kvStore[args.Key]
	if success {
		ret.V = v
		ret.Status = raft.Status_KeyFound
	} else {
		ret.Status = raft.Status_KeyNotFound
	}
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	// TODO: Implement this!
	currentTerm := rn.getCurrentTerm()
	reply := raft.RequestVoteReply{
		From:        args.To,
		To:          args.From,
		Term:        currentTerm,
		VoteGranted: false,
	}

	if args.Term < currentTerm {
		return &reply, nil
	}

	if args.Term > currentTerm {
		rn.setRole(raft.Role_Follower)
		rn.setCurrentTerm(args.Term)
		rn.setVotedFor(-1)
		reply.Term = args.Term
	}

	lastLogIndex, lastLogTerm := rn.getLastEntry()
	votedFor := rn.getVotedFor()

	up_to_date := (lastLogTerm > args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex)
	if (votedFor == -1 || votedFor == args.CandidateId) && !up_to_date {
		rn.setVotedFor(args.CandidateId)
		reply.VoteGranted = true
	}

	if reply.VoteGranted {
		rn.resetCurElectionTicker <- true
	}
	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	// TODO: Implement this
	currentTerm := rn.getCurrentTerm()
	reply := raft.AppendEntriesReply{
		From:       args.To,
		To:         args.From,
		Term:       currentTerm,
		Success:    false,
		MatchIndex: rn.getCommitIndedx(),
	}

	if args.Term < currentTerm {
		return &reply, nil
	}

	rn.resetCurElectionTicker <- true
	rn.leaderId = args.LeaderId

	if (args.Term > currentTerm) || (rn.getRole() != raft.Role_Follower) {
		rn.setRole(raft.Role_Follower)
		rn.setCurrentTerm(args.Term)
		rn.setVotedFor(-1)
		reply.Term = args.Term
	}

	lastLogIndex, _ := rn.getLastEntry()

	log_match := lastLogIndex >= args.PrevLogIndex && rn.log[args.PrevLogIndex].Term == args.PrevLogTerm
	if !log_match {
		return &reply, nil
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + int32(i) + 1
		if index > lastLogIndex {
			rn.log = append(rn.log, entry)
		} else if rn.log[index].Term != entry.Term {
			rn.log = rn.log[:index]
			rn.log = append(rn.log, args.Entries[index])
		}
	}

	printf(">>> [%d] leaderCommit: %d, myCommit: %d, log: %v, lastLogId: %d", rn.myId, args.LeaderCommit, rn.getCommitIndedx(), rn.log, rn.getLastLogIndex())
	if (args.LeaderCommit > rn.getCommitIndedx()) && (rn.getLastLogIndex() > rn.getCommitIndedx()) {
		startCommitId := rn.getCommitIndedx()
		if args.LeaderCommit < rn.getLastLogIndex() {
			rn.setCommitIndex(args.LeaderCommit)
		} else {
			rn.setCommitIndex(rn.getLastLogIndex())
		}
		endCommitId := rn.getCommitIndedx()
		printf(">>> [%d] comit from %d to %d, logs: %v", rn.myId, startCommitId, endCommitId, rn.log)
		for i := startCommitId + 1; i <= endCommitId; i++ {
			logEntry := rn.log[i]
			if logEntry.Op == raft.Operation_Put {
				rn.kvStore[logEntry.Key] = logEntry.Value
			} else {
				delete(rn.kvStore, logEntry.Key)
			}
			printf("[%d] store: %v, commit %v", rn.myId, rn.kvStore, logEntry)
		}
	}
	reply.MatchIndex = rn.getLastLogIndex()
	reply.Success = true
	return &reply, nil
}

func (rn *raftNode) RandomElectionTimeout(electionTimeoutTime int) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rand_time := r.Intn(90) // electionTimeoutTime)
	timeout := time.Duration(electionTimeoutTime+rand_time) * time.Millisecond
	return timeout
}

func (rn *raftNode) ElectionTicker(electionTimeout int) {
	for {
		dur := rn.RandomElectionTimeout(electionTimeout)
		select {
		case <-rn.resetCurElectionTicker:
			continue
		case <-rn.stopCurElectionTicker:
			return
		case <-time.After(dur):
			role := rn.getRole()
			if role != raft.Role_Leader {
				if role != raft.Role_Candidate {
					rn.setRole(raft.Role_Candidate)
				} else {
					rn.stopCurElection <- true
				}
				go rn.StartLeaderElection()
			}
		}
	}
}

func (rn *raftNode) HeartBeatTicker(heartBeatInterval int) {
	rn.heartBeatInterval = heartBeatInterval
	for {
		select {
		case <-rn.stopCurHeartBeatTicker:
			return
		case <-rn.resetCurHeartBeatTicker:
			continue
		case <-time.After(time.Duration(heartBeatInterval) * time.Millisecond):
			role := rn.getRole()
			if role == raft.Role_Leader {
				rn.notifyHearBeat <- true
			}
		}
	}
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	// TODO: Implement this!
	// printf("[%d] reset timeout to %d ms", rn.myId, args.Timeout)
	rn.stopCurElectionTicker <- true
	go rn.ElectionTicker(int(args.Timeout))

	var reply raft.SetElectionTimeoutReply
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatTimeOUT(ctx context.Context, args *raft.SetHeartBeatTimeOUTArgs) (*raft.SetHeartBeatTimeOUTReply, error) {
	// TODO: Implement this!
	// printf("[%d] reset heartbeat to %d ms", rn.myId, args.Interval)
	rn.stopCurHeartBeatTicker <- true
	go rn.HeartBeatTicker(int(args.Interval))

	var reply raft.SetHeartBeatTimeOUTReply
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
