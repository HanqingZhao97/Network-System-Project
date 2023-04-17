package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore
	// consistent hashing ring
	ConsistentHashRing *ConsistentHashRing
	// index of highest log entry known to be committed
	commitIdx      int64
	pendingCommits []chan bool

	// index of highest log entry applied to state machine
	lastEntry int64

	// Leader related fields
	nextIndex  []int64
	matchIndex []int64

	// Server Info
	ipList   []string
	serverId int64

	mu sync.Mutex

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isCrashed {
		s.isLeader = false
		return nil, ERR_SERVER_CRASHED
	}

	// Node is not leader, return an Error
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	return &FileInfoMap{
		FileInfoMap: s.metaStore.FileMetaMap,
	}, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	//panic("todo")
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Println("Inside Get Block Store Map")
	//fmt.Println("GetBlockStoreMap Server ", s.serverId, "is leader? ", s.isLeader)
	if s.isCrashed {
		s.isLeader = false
		return nil, ERR_SERVER_CRASHED
	}

	// not leader, return an Error
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	ring := s.ConsistentHashRing
	blockMap := make(map[string]*BlockHashes)
	for _, hash := range hashes.Hashes {
		// get the responsible server
		responServer := ring.GetResponsibleServer(hash)
		if blockMap[responServer] == nil {
			blockMap[responServer] = &BlockHashes{Hashes: []string{}}
		}
		blockMap[responServer].Hashes = append(blockMap[responServer].Hashes, hash)
	}
	return &BlockStoreMap{BlockStoreMap: blockMap}, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Println("Inside Get Block Store addr")
	//fmt.Println("GetBlockStoreAddr Server ", s.serverId, "is leader? ", s.isLeader)
	if s.isCrashed {
		s.isLeader = false
		return nil, ERR_SERVER_CRASHED
	}

	// Node is not leader, return an Error
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	return &BlockStoreAddrs{BlockStoreAddrs: s.metaStore.BlockStoreAddrs}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if s.isCrashed {
		s.isLeader = false
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	// Append to the current logs
	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)
	commited := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, commited)

	//fmt.Println("Before Attempt Commit~~~~~~~~~~~", filemeta)

	s.mu.Lock()
	defer s.mu.Unlock()

	go s.tryToCommit()
	success := <-commited
	if success {
		//fmt.Printf("Leader %d is updating file, the file content is \n", s.serverId)
		//fmt.Println(filemeta)
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return &Version{
		Version: filemeta.Version,
	}, nil
}

func (s *RaftSurfstore) tryToCommit() {
	if s.isCrashed {
		s.isLeader = false
		return
	}

	targetIdx := s.commitIdx + 1

	//fmt.Println("targetIdx: ", targetIdx, " s.commitIdx: ", s.commitIdx, " log length: ", len(s.log))

	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.makeCommit(int64(idx), targetIdx, commitChan)
	}

	// Check if the majority already updated the change
	commitCount := 1
	for {
		if s.isCrashed {
			s.isLeader = false
			return
		}
		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}

		// Update the fields when majority servers updated the file
		if commitCount > len(s.ipList)/2 {
			s.pendingCommits[len(s.pendingCommits)-1] <- true
			s.commitIdx = targetIdx
			break
		}
	}
}

// Helper method to update file entry
func (s *RaftSurfstore) makeCommit(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {

	for {
		if s.isCrashed {
			s.isLeader = false
			return
		}

		conn, err := grpc.Dial(s.ipList[serverIdx], grpc.WithInsecure())
		if err != nil {
			return
		}

		client := NewRaftSurfstoreClient(conn)

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      s.log[:entryIdx],
			LeaderCommit: s.commitIdx,
		}

		if s.matchIndex[serverIdx] > 0 {
			input.PrevLogIndex = s.matchIndex[serverIdx]
			input.PrevLogTerm = s.log[s.matchIndex[serverIdx]-1].Term
		}

		// If the followers crash, conitnue to try
		for {
			if s.isCrashed {
				fmt.Println(serverIdx, " crashed in makeCommit")
				s.isLeader = false
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			fmt.Println("Commit Entry inside go routine, sending to ", serverIdx)
			output, err := client.AppendEntries(ctx, input)

			if err == nil && output.Success {
				s.matchIndex[serverIdx] = output.MatchedIndex
				s.nextIndex[serverIdx] = output.MatchedIndex + 1
				commitChan <- output
				return
			}
		}
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIdx, set commitIdx = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	output := &AppendEntryOutput{
		ServerId:     s.serverId,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}

	if s.isCrashed {
		s.isLeader = false
		//fmt.Println("Inside append entries, server ", s.serverId, "is crashed", "return output: ", output)
		return output, ERR_SERVER_CRASHED
	}

	// Reply false if term < currentTerm
	if input.Term < s.term {
		return output, nil
	}

	// Revert to follower if the input term is >= current term
	if input.Term >= s.term {
		if s.isLeader {
			s.isLeader = false
		}
		s.term = input.Term
	}
	// write logs  Append any new entries not already in the log
	if len(input.Entries) > 0 {
		fmt.Println("Receive append Entries ------------")
		fmt.Println("Server ID", s.serverId)
		fmt.Println("My log: ", s.log)
		fmt.Println("Received Entries: ", input.Entries)
		fmt.Println("prevlogIndex: ", input.PrevLogIndex, " prevlogTerm: ", input.PrevLogTerm)

		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		if input.PrevLogIndex > int64(len(s.log)) {
			return output, nil
		}

		s.log = append(s.log[:input.PrevLogIndex], input.Entries[input.PrevLogIndex:]...)

		fmt.Println("After append: ", s.log)
		fmt.Println("End --------------")
	}

	if input.LeaderCommit > s.commitIdx {
		s.commitIdx = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log))))
	}
	for s.lastEntry < s.commitIdx {
		entry := s.log[s.lastEntry]
		s.lastEntry++

		fmt.Println("Server ", s.serverId, " is updating ", entry.FileMetaData, " at last applied ", s.lastEntry)
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	output.Success = true
	output.MatchedIndex = s.lastEntry

	return output, nil
}

// Emulates elections, sets the node to be the leader
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Increment the current term
	s.term++

	s.isLeader = true

	//s.log = s.log[:s.commitIdx]

	//fmt.Printf("Setting Leader %d to have term %d \n", s.serverId, s.term)

	// Reset nextIndex and matchIndex
	for idx := range s.nextIndex {
		s.nextIndex[idx] = s.commitIdx + 1
		s.matchIndex[idx] = 0
	}
	return &Success{Flag: true}, nil
}

// Sends a round of AppendEntries to all other nodes. The leader will attempt to replicate logs
// to all other nodes when this is called. It can be called even when there are no entries to replicate.
// If a node is not in the leader state it should do nothing.
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		s.isLeader = false
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return &Success{
			Flag: false,
		}, ERR_NOT_LEADER
	}

	numCrashed := 0
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		//fmt.Printf("Server %d in term %d is Sending heartbeat to server %d!!\n", s.serverId, s.term, idx)
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, nil
		}
		client := NewRaftSurfstoreClient(conn)
		// create correct AppendEntryInput from s.nextIndex
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      s.log,
			LeaderCommit: s.commitIdx,
		}

		if s.matchIndex[idx] > 0 {
			input.PrevLogIndex = s.matchIndex[idx]
			input.PrevLogTerm = s.log[s.matchIndex[idx]-1].Term
		}

		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			output, err := client.AppendEntries(ctx, input)

			// Follower crashed
			if err != nil {
				numCrashed++
				break
			} else if !output.Success && output.Term >= s.term {
				s.isLeader = false
				break
			} else if !output.Success { // If the prev Index does not exist
				input.PrevLogIndex--
			} else { // If no error
				s.matchIndex[idx] = output.MatchedIndex
				//fmt.Println("Server ", idx, "'s match index is now ", s.matchIndex[idx])
				break
			}
		}
	}

	// If majority of the server crashed
	if numCrashed > len(s.ipList)/2 {
		return &Success{Flag: false}, nil
	}
	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
