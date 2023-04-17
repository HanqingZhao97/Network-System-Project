package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,

		// custom initialize
		ipList:   config.RaftAddrs,
		serverId: id,

		commitIdx:      0,
		pendingCommits: make([]chan bool, 0),
		lastEntry:      0,

		nextIndex:  make([]int64, len(config.RaftAddrs)),
		matchIndex: make([]int64, len(config.RaftAddrs)),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	//panic("todo")
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)
	//start server and listern
	lis, err := net.Listen("tcp", server.ipList[server.serverId])
	if err != nil {
		return err
	}
	return s.Serve(lis)
}
