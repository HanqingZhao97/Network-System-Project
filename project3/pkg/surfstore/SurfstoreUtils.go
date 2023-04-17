package surfstore

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// create some global variables to store baseDir and Index infos
var baseDirData = make(map[string]*Block)
var baseDirHashes = make(map[string][]string)
var indexMap = make(map[string]*FileMetaData)
var blockStoreAddr string

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	//panic("todo")
	// store all the file names
	filenames := make(map[string]bool)
	// get block address
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	// Scan and compute base file's hash list
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {

		// Skip index.db
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		}

		// Open base directory files
		f, err := os.Open(ConcatPath(client.BaseDir, file.Name()))
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		// Computing the hash value for each
		for {
			byteChunk := make([]byte, client.BlockSize)
			nByte, err := f.Read(byteChunk)
			if err != nil {
				break
			}

			// Create a new localBlock and assign the values in
			block := new(Block)
			block.BlockData = make([]byte, nByte)
			copy(block.BlockData, byteChunk[:nByte])
			block.BlockSize = int32(nByte)

			// Compute hash value
			hash := GetBlockHashString(byteChunk[:nByte])

			// Add into the data structure
			baseDirData[hash] = block
			baseDirHashes[file.Name()] = append(baseDirHashes[file.Name()], hash)
		}
		filenames[file.Name()] = true
	}

	//Read from local index.db
	if indexMap, err = LoadMetaFromMetaFile(client.BaseDir); err != nil {
		log.Fatal(err)
	}

	//Connect the server, download an updated FileInfoMap
	var remoteInfos map[string]*FileMetaData
	if err := client.GetFileInfoMap(&remoteInfos); err != nil {
		log.Fatal(err)
	}

	// Add all the remote file info into the filenames set
	for fileName := range remoteInfos {
		filenames[fileName] = true
	}

	for fileName := range filenames {

		// generate vriables to handle conflicts
		base_hash, base_exist := baseDirHashes[fileName]
		index_con, index_exist := indexMap[fileName]
		remote_con, remote_exist := remoteInfos[fileName]

		remote_deleted := false

		// If the files are deleted, mark them as do not exist
		if (index_exist) && (len(index_con.BlockHashList) == 1) && (index_con.BlockHashList[0] == "0") {
			index_exist = false
		}
		if (remote_exist) && (len(remote_con.BlockHashList) == 1 && (remote_con.BlockHashList[0]) == "0") {
			remote_exist = false
			remote_deleted = true
		}

		// Check if the hashes are different
		var block_index_hashDiff bool
		var index_remote_hashDiff bool

		// block and index hashes check
		if base_exist && index_exist {
			block_index_hashDiff = HashEqual(base_hash, index_con.BlockHashList)
		} else if base_exist == index_exist {
			block_index_hashDiff = true
		} else {
			block_index_hashDiff = false
		}

		// index and remote hashes check
		if index_exist && remote_exist {
			index_remote_hashDiff = HashEqual(index_con.BlockHashList, remote_con.BlockHashList)
		} else if index_exist == remote_exist {
			index_remote_hashDiff = true
		} else {
			index_remote_hashDiff = false
		}

		// If local and remote are different in changes
		if !block_index_hashDiff && index_remote_hashDiff {

			// Create a new File Meta Data for this
			fileMeta := new(FileMetaData)
			fileMeta.Filename = fileName

			// If already exists in remote, have a version of +1, if not have a version of 1
			if remote_exist {
				fileMeta.Version = remote_con.Version + 1
			} else if remote_deleted {
				fileMeta.Version = remote_con.Version + 1
			} else {
				fileMeta.Version = 1
			}

			// If deleted , have a fileMeta BlockList of "0"
			if !base_exist {
				fileMeta.BlockHashList = make([]string, 0)
				fileMeta.BlockHashList = append(fileMeta.BlockHashList, "0")
			} else {
				fileMeta.BlockHashList = make([]string, len(base_hash))
			}

			// If new file created in base dir, or if a file is modified in local
			if (base_exist && !index_exist && !remote_exist) || (base_exist && index_exist && remote_exist) {

				if err = uploadToRemote(&client, fileName); err != nil {
					log.Fatal(err)
				}

				// Deepcoyp to update the hashlist
				copy(fileMeta.BlockHashList, base_hash)
			}

			// Update the file version
			var newVersion int32
			if err := client.UpdateFile(fileMeta, &newVersion); err != nil {
				log.Fatal("error updating file")
			}

			fmt.Println("Version", newVersion)
			if newVersion == -1 {
				// Handle Race Condition

				fmt.Println("Race Condition")

				var updatedFileInfos map[string]*FileMetaData
				if err := client.GetFileInfoMap(&updatedFileInfos); err != nil {
					log.Fatal(err)
				}

				if (len(updatedFileInfos[fileName].BlockHashList) == 1) && (updatedFileInfos[fileName].BlockHashList[0] == "0") {
					os.Remove(ConcatPath(client.BaseDir, fileName))
				} else {
					// Sync the most updated remote block to local
					if err := downloadToLocal(&client, updatedFileInfos[fileName]); err != nil {
						log.Fatal(err)
					}
				}

				fileMeta = updatedFileInfos[fileName]

			}

			// Update the indexMap
			indexMap[fileName] = fileMeta

		} else if !index_remote_hashDiff {
			// Server change

			if remote_exist {
				// If did not "delete" the file, sync from remote to local
				if err := downloadToLocal(&client, remote_con); err != nil {
					log.Fatal(err)
				}
			} else if base_exist {
				// If remote deleted it local exist, then delete it in local
				os.Remove(ConcatPath(client.BaseDir, remote_con.Filename))
			}

			// Update the indexMap
			indexMap[fileName] = remote_con

		}
	}

	// Write the most updated index.db once done synchronizing
	if err := WriteMetaFile(indexMap, client.BaseDir); err != nil {
		log.Fatal(err)
	}

}

/* This function checks if two hash list are the same*/
func HashEqual(index_H []string, remote_H []string) bool {
	if len(index_H) != len(remote_H) {
		return false
	}
	for i, h := range index_H {
		if h != remote_H[i] {
			return false
		}
	}
	return true
}

/* This function update the remote to match the local file */
func uploadToRemote(client *RPCClient, fileName string) error {

	var hash_remote []string
	// Find all the blocks that are in the local file
	if err := client.HasBlocks(baseDirHashes[fileName], blockStoreAddr, &hash_remote); err != nil {
		log.Fatal(err)
	}

	ends_slice := len(hash_remote) <= 0
	idx := 0

	for _, hash := range baseDirHashes[fileName] {

		// If same hash exist in both local and remote
		if !ends_slice && (hash == hash_remote[idx]) {
			// SKip them
			idx++
			if idx == len(hash_remote) {
				ends_slice = true
			}
		} else {

			// If not in remote, put block
			block := new(Block)
			block.BlockData = make([]byte, baseDirData[hash].BlockSize)
			copy(block.BlockData, baseDirData[hash].BlockData)
			block.BlockSize = baseDirData[hash].BlockSize

			var succ Success
			if err := client.PutBlock(block, blockStoreAddr, &succ.Flag); err != nil {
				log.Fatal("error when putting block")
			}
		}
	}

	return nil
}

/* This function update the local file to match the remote file */
func downloadToLocal(client *RPCClient, remoteMeta *FileMetaData) error {

	filePath := ConcatPath(client.BaseDir, remoteMeta.Filename)

	block_HUni := make([]string, 0)
	// check if file already exist
	if _, err := os.Stat(filePath); err == nil || !os.IsNotExist(err) {
		// If the file exist: remove it
		os.Remove(filePath)

		// Get the blocks
		if err := client.HasBlocks(baseDirHashes[remoteMeta.Filename], blockStoreAddr, &block_HUni); err != nil {
			log.Fatal("error generating block")
		}

	}

	ends := len(block_HUni) <= 0
	idx := 0

	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal("error creating file")
	}

	w := bufio.NewWriter(file)
	for _, hash := range remoteMeta.BlockHashList {

		if !ends && (hash == block_HUni[idx]) {

			// If found, directly write to file
			idx++
			ends = (idx == len(block_HUni))
			w.WriteString(string(baseDirData[hash].BlockData))

		} else {

			// If not exist then get block from remote
			block := new(Block)
			if err := client.GetBlock(hash, blockStoreAddr, block); err != nil {
				log.Fatal("error when getting block")
			}
			w.WriteString(string(block.BlockData))
		}
	}

	w.Flush()

	return nil
}
