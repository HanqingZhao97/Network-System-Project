package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	//panic("todo")
	hashes := []string{}
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	// find the first server with larger hash value than blockHash
	blockHash := c.Hash(blockId)
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockHash {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}
	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	//panic("todo")
	// hash servers on a hash ring
	consistentHashRing := make(map[string]string) // hash: serverName
	c := &ConsistentHashRing{ServerMap: consistentHashRing}
	for _, serverName := range serverAddrs {
		serverHash := c.Hash("blockstore" + serverName)
		consistentHashRing[serverHash] = serverName
	}
	return c
}
