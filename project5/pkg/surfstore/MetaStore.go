package surfstore

import (
	context "context"
	"fmt"
	"log"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
	mu sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	//if metadata is nil return error
	if fileMetaData == nil {
		log.Fatal("fileMetaData is nil")
	}
	old, found := m.FileMetaMap[fileMetaData.Filename]
	if found {
		fmt.Println(fileMetaData.Filename, "old version ", old.Version, "new version ", fileMetaData.Version)
		if old.Version+1 != fileMetaData.Version {
			PrintMetaMap(m.FileMetaMap)
			return &Version{Version: -1}, nil
		} else { //update the file version
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
			PrintMetaMap(m.FileMetaMap)
			return &Version{Version: fileMetaData.Version}, nil
		}
	}
	//Use UpdateFile()??
	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	m.FileMetaMap[fileMetaData.Filename].Version = 1
	PrintMetaMap(m.FileMetaMap)

	return &Version{Version: 1}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ring := m.ConsistentHashRing
	blockMap := make(map[string]*BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		// get the responsible server
		responServer := ring.GetResponsibleServer(hash)
		if blockMap[responServer] == nil {
			blockMap[responServer] = &BlockHashes{Hashes: []string{}}
		}
		blockMap[responServer].Hashes = append(blockMap[responServer].Hashes, hash)
	}
	return &BlockStoreMap{BlockStoreMap: blockMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	//fail?
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
