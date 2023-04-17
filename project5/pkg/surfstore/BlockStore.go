package surfstore

import (
	context "context"
	"fmt"
	"log"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer

	mu sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if blockHash == nil {
		log.Fatal("blockHash is nil")
	}

	block, found := bs.BlockMap[blockHash.Hash]
	fmt.Println(blockHash.Hash, found)
	if !found {
		log.Fatal("Not found in block map")
	}
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	//panic("todo")
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if block == nil {
		log.Fatal("Block is nil")
	}
	//Hash the block content
	h := GetBlockHashString(block.BlockData)
	bs.BlockMap[h] = block
	return &Success{}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	//panic("todo")
	bs.mu.Lock()
	defer bs.mu.Unlock()
	//see if "in" is availble
	if blockHashesIn == nil {
		log.Fatal("blockHashesIn is nil")
	}
	subset_list := new(BlockHashes)
	for _, in := range blockHashesIn.Hashes {
		_, found := bs.BlockMap[in]
		if found {
			subset_list.Hashes = append(subset_list.Hashes, in)
		}
	}
	return subset_list, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	//panic("todo")
	bs.mu.Lock()
	defer bs.mu.Unlock()

	blockHashes := new(BlockHashes)
	for hash := range bs.BlockMap {
		blockHashes.Hashes = append(blockHashes.Hashes, hash)
	}
	return blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
