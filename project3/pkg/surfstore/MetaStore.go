package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
	mu sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	m.mu.Lock()
	defer m.mu.Unlock()

	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	//panic("todo")
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

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	//fail?
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
