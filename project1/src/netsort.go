package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net"
	"time"
	"math"
	"bufio"
	"sort"
	"io"
	"os"
	"strconv"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}
//store client specific records for partitions
type Clients struct {
	Record []byte
	Conn net.Conn
}

/* Constants */
const(
	Proto = "tcp" //transport protocol to be used
	//WorldSize = 16 //number of clients that we expect to receive connections
	MaxMessageSize = 100 //the maximum size of a message we expect to receive
)

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func listenForClientConnections(
	write_only_ch chan<- Clients, // a channel to which client messages will be sent
	host string, port string) {
	server_address := host + ":" + port
	listener, err := net.Listen(Proto, server_address)
	if err != nil {
		log.Panic(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error connecting: ", err.Error())
			continue
		}
		// Spawn a new goroutine to handle this client's connections
		// and go back to listening for additional connections
		go handleClientConnection(conn, write_only_ch)
	}
}

func handleClientConnection(
	conn net.Conn, // the connection to handle
	write_only_ch chan<- Clients, // a channel to which incoming messages from this client will be written
) {
	var get []byte
	for {
		client_msg_buf := make([]byte, MaxMessageSize)
	    bytes_read, err := conn.Read(client_msg_buf)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Panicln(err)
		} else {
			data := client_msg_buf[:bytes_read]
			get = append(get, data...)
		}
	}
	write_only_ch <- Clients{get, conn}
}

func handleSendConnection(host string, port string, record []byte){
	var conn net.Conn
	var err error
	//dial multiple times for a short delay
	for i := 0; i < 10; i++{
		conn, err = net.Dial(Proto, host+":"+port)
		if err != nil{
			log.Println("Cannot dial: ", err)
			time.Sleep(100 * time.Millisecond)
		}else{
			break
		}
	}
	defer conn.Close()
	_, err = conn.Write(record)
	if err != nil{
		fmt.Fprintf(os.Stderr, "Conn::Read: err %v\n", err)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/
	write_ch := make(chan Clients)
	for _, server := range scs.Servers{
		if serverId == server.ServerId{
			//goroutine
			go listenForClientConnections(write_ch, server.Host, server.Port)
			break
		}
	}
	//wait a few seconds for a short delay
	time.Sleep(200 * time.Millisecond)
	
	//read file 
	input_path := os.Args[2]
	in_File, err := os.Open(input_path)
	if err != nil{
		log.Fatalf("Invalid read file, must be an int %v", err)
	}

	//log2 of total num of server = number of bits
	num_bits := int(math.Log2(float64(len(scs.Servers))))

	buf := make([]byte, 100) 
	for {
		record := make([]byte, MaxMessageSize)
		n, err := in_File.Read(buf)
		if err != nil && err != io.EOF{
			log.Fatalf("Invalid input file %v", err)
		}
		if err == io.EOF {
			break
		}
		copy(record, buf[0:n])
		//records = append(records, buf[:n])
		//since 8 bit is 1 byte, calculate the current id using division
		curr_serverId := int(record[0] >> (8-num_bits))
		for _, server := range scs.Servers{
			if curr_serverId == server.ServerId{
				handleSendConnection(server.Host, server.Port, record)
				break
			}
		}
	
	}
	in_File.Close()

	//track the completeness
	trackChan := []byte{}
	for i := 0; i < MaxMessageSize; i++{
		//all zeros if complete
		trackChan = append(trackChan, 0)
	}
	for _, server := range scs.Servers {
		handleSendConnection(server.Host, server.Port, trackChan)
	}

	//track the number of servers completed
	num_of_complete := 0
	//read records and sort
	records := [][]byte{}
	for {
		if num_of_complete == len(scs.Servers) {
			break
		}
		client := <-write_ch
		is_complete := true
		for _, Byte := range client.Record {
			//if all bytes are 0, it means it was sent
			if Byte != 0 {
				is_complete = false
				break
			}
		}
		if is_complete {
			num_of_complete += 1
		} else {
			records = append(records, client.Record)
		}
	}
	//sort the records the same in proj0
	sort.Slice(records, func(i, j int) bool {
		return string(records[i][:10]) < string(records[j][:10])
	})
	//write to output files
	output_path := os.Args[3]
	out_File, err := os.Create(output_path)
	if err != nil {
		log.Fatalf("Invalid output file %v", err)
	}
	writer := bufio.NewWriter(out_File)
	for _, record := range records{
		_, err = writer.Write(record)
		if err != nil{
			log.Fatalf("Invalid writer %v", err)
		}
		writer.Flush()
	}
	out_File.Close()
}
