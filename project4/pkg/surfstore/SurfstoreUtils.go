package surfstore

import (
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	"sort"
	"strings"
)

func ClientSync(client RPCClient) {

	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal("Error reading basedir")
	}
	localMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatal("Could not load meta from meta file")
	}

	baseDirHashes := make(map[string][]string)
	for _, file := range files {
		// check filename
		if file.Name() == DEFAULT_META_FILENAME || strings.Contains(file.Name(), ",") || strings.Contains(file.Name(), "/") {
			continue
		}

		// get blocks numbers
		var blocksNum int = int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))
		f, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			log.Fatal("Error reading file: ", err)
		}

		defer f.Close()
		var hashes []string
		byteChunk := make([]byte, client.BlockSize)
		for i := 0; i < blocksNum; i++ {

			bByte, err := f.Read(byteChunk)
			if err != nil {
				log.Fatal("Error reading bytes from file: ", err)
			}

			hash := GetBlockHashString(byteChunk[:bByte])
			hashes = append(hashes, hash)
		}
		baseDirHashes[file.Name()] = hashes
	}

	for filename, hashes := range baseDirHashes {
		// check new file & update
		if localMap[filename] == nil {
			localMap[filename] = &FileMetaData{Filename: filename, Version: int32(1), BlockHashList: hashes}
			// check changed file
		} else if !reflect.DeepEqual(localMap[filename].BlockHashList, hashes) {
			localMap[filename].BlockHashList = hashes
			localMap[filename].Version = localMap[filename].Version + 1
		}
	}

	for fileName, fileMetaData := range localMap {
		// update the deleted file
		if _, ok := baseDirHashes[fileName]; !ok {
			if len(fileMetaData.BlockHashList) != 1 || fileMetaData.BlockHashList[0] != "0" {
				fileMetaData.Version++
				fileMetaData.BlockHashList = []string{"0"}
			}
		}
	}

	var addrs []string
	if err := client.GetBlockStoreAddrs(&addrs); err != nil {
		log.Fatal("Could not get blockStoreAddr: ", err)
	}

	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		log.Fatal("Could not get remote index: ", err)
	}
	// loop in the local index map
	for filename, localMetaData := range localMap {
		m := make(map[string][]string)
		err = client.GetBlockStoreMap(baseDirHashes[filename], &m)
		if err != nil {
			log.Fatal("Could not get blockStoreMap: ", err)
		}
		if remoteMetaData, ok := remoteIndex[filename]; ok {
			if localMetaData.Version > remoteMetaData.Version {
				uploadToRemote(client, localMetaData, addrs, m)
			}
		} else {
			uploadToRemote(client, localMetaData, addrs, m)
		}
	}
	for filename, remoteMetaData := range remoteIndex {
		m := make(map[string][]string)
		err = client.GetBlockStoreMap(baseDirHashes[filename], &m)
		if err != nil {
			log.Fatal("Could not get blockStoreAddr: ", err)
		}
		if localMetaData, ok := localMap[filename]; !ok {
			localMap[filename] = &FileMetaData{}
			downloadToLocal(client, localMap[filename], remoteMetaData, m)
		} else {
			if remoteMetaData.Version >= localMap[filename].Version {
				downloadToLocal(client, localMetaData, remoteMetaData, m)
			}
		}
	}

	WriteMetaFile(localMap, client.BaseDir)
}

func uploadToRemote(client RPCClient, FileMD *FileMetaData, addrs []string, blockStoreMap map[string][]string) error {
	path := client.BaseDir + "/" + FileMD.Filename // local file path

	var latestVersion int32
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := client.UpdateFile(FileMD, &latestVersion)
		if err != nil {
			log.Fatal("Could not update file")
		} else {
			FileMD.Version = latestVersion
		}
		log.Fatal("Couldn't open Os")
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		log.Fatal("Error opening file: ", err)
	}
	defer file.Close()

	fileInfo, err := os.Stat(path)
	if err != nil {
		log.Fatal("Error geting fileInfo: ", err)
	}

	var blockNum int = int(math.Ceil(float64(fileInfo.Size()) / float64(client.BlockSize)))
	for i := 0; i < blockNum; i++ {
		byteChunk := make([]byte, client.BlockSize)
		bByte, err := file.Read(byteChunk)
		if err != nil {
			log.Fatal("Error reading bytes from file in basedir: ", err)
		}
		byteChunk = byteChunk[:bByte]
		//create new block
		block := Block{BlockData: byteChunk, BlockSize: int32(bByte)}
		hash := GetBlockHashString(block.BlockData)
		//fine corresponding responsible server
		var responsibleSever string
		for bsAddr, blockHashes := range blockStoreMap {
			for _, blockHash := range blockHashes {
				if hash == blockHash {
					responsibleSever = bsAddr
				}
			}
		}

		var succ Success
		if err := client.PutBlock(&block, strings.ReplaceAll(responsibleSever, "blockstore", ""), &succ.Flag); err != nil {
			log.Fatal("Could not put block")
		}
	}

	if err := client.UpdateFile(FileMD, &latestVersion); err != nil {
		log.Fatal("Could not update file")
	}

	FileMD.Version = latestVersion
	return nil
}

func downloadToLocal(client RPCClient, localMetaData *FileMetaData, remoteMetaData *FileMetaData, blockStoreMap map[string][]string) error {
	path := client.BaseDir + "/" + remoteMetaData.Filename // local file path

	file, err := os.Create(path)
	if err != nil {
		log.Fatal("Error creating file")
	}
	defer file.Close()

	_, err = os.Stat(path)
	if err != nil {
		log.Fatal("Error geting fileInfo")
	}

	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
		if err := os.Remove(path); err != nil {
			log.Fatal("Could not remove file")
		}
		*localMetaData = *remoteMetaData
		return nil
	}
	m := make(map[string][]string)
	err = client.GetBlockStoreMap(remoteMetaData.BlockHashList, &m)
	if err != nil {
		log.Fatal("Could not get blockStoreAddr: ", err)
	}
	// order the block map's key
	keys := []string{}
	for k, _ := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	output := ""
	for _, hash := range remoteMetaData.BlockHashList {
		for _, key := range keys {
			for _, blockHash := range m[key] {

				if hash == blockHash {
					var block Block
					if err := client.GetBlock(hash, strings.ReplaceAll(key, "blockstore", ""), &block); err != nil {
						log.Fatal("Could not get block")
					}
					output += string(block.BlockData)
				}
			}
		}
	}

	if _, err := file.WriteString(output); err != nil {
		log.Fatal("Could not write to file:")
	}

	*localMetaData = *remoteMetaData
	return nil
}
