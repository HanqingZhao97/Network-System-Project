package tritonhttp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"os"
	"path/filepath"
	"time"
)

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"

	// VirtualHosts contains a mapping from host name to the docRoot path
	// (i.e. the path to the directory to serve static files from) for
	// all virtual hosts that this server supports
	VirtualHosts map[string]string
}

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.
func (s *Server) ListenAndServe() error {

	// Hint: Validate all docRoots

	// Hint: create your listen socket and spawn off goroutines per incoming client
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	fmt.Println("Listening ", ln.Addr())

	// Close the connection when exit
	defer func() {
		err = ln.Close()
		if err != nil {
			fmt.Println("error closing listener socket", err)
		}
	}()

	// Handle the received connection
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		fmt.Println("accept connecction", conn.RemoteAddr())
		go s.HandleConnection(conn)
	}
}

func (s *Server) HandleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)

	for {
		//timeout 5 seconds
		err := conn.SetReadDeadline(time.Now().Add(CONNECT_TIMEOUT))
		if err != nil {
			log.Printf("Fail to set timeout for connection %v", conn)
			_ = conn.Close()
			return
		}
		//read next
		rqst, received, err := ReadRequest(reader)

		//handle errors
		if err != nil {
			// Handle EOF
			if errors.Is(err, io.EOF) {
				log.Printf("Connection EOF error %v", conn.RemoteAddr())
				_ = conn.Close()
				return
			}

			// Timeout
			if Err, t := err.(net.Error); t && Err.Timeout() {
				if received {
					fmt.Println("Request is time out")
					response := &Response{}
					response.HandleBadRequest()
					_ = response.Write(conn)
				}
				log.Printf("Connection time out %v, closing connection", conn.RemoteAddr())
				_ = conn.Close()
				return
			}

			//other errors
			log.Printf("Handle other error: %v", err)
			response := &Response{}
			response.HandleBadRequest()
			_ = response.Write(conn)
			_ = conn.Close()
			return
		}

		//handle other good requests
		log.Printf("Successful Request: %v", rqst)
		response := s.HandleSuccessRequest(rqst)
		err = response.Write(conn)
		if err != nil {
			fmt.Println(err)
		}

		//close if requested
		if rqst.Close && conn != nil {
			fmt.Println("Requested Closing Connection")
			_ = conn.Close()
			return
		}

	}
}

// 400 Bad Request Response
func (res *Response) HandleBadRequest() {
	res.Request = nil
	res.Headers = make(map[string]string)
	res.Proto = "HTTP/1.1"
	res.StatusCode = 400
	res.StatusText = "Bad Request"
	res.FilePath = ""

	currentTime := FormatTime(time.Now())
	res.Headers["Date"] = currentTime
	res.Headers["Connection"] = "close"
}

// Handle Good Response
func (s *Server) HandleSuccessRequest(rqst *Request) (res *Response) {
	res = &Response{}
	url_length := len(rqst.URL)

	//add index.html after if ends with /
	if rqst.URL[url_length-1:] == "/" {
		rqst.URL = filepath.Clean(rqst.URL + "index.html")
	}

	//get the path from VirtualHosts
	path := filepath.Join(s.VirtualHosts[rqst.Host], rqst.URL)
	//handle not found
	_, Err := os.Stat(path)
	if os.IsNotExist(Err) {
		fmt.Println("Not Found Path")
		res.HandleNotFound(rqst)
		return res
	}
	//handle ok
	res.HandleOK(rqst, path)
	return res

}

// 404 Not Found Response
func (res *Response) HandleNotFound(rqst *Request) {
	res.Proto = "HTTP/1.1"
	res.StatusCode = 404
	res.StatusText = "Not Found"
	res.FilePath = ""
	res.Request = nil
	res.Headers = make(map[string]string)

	currentTime := FormatTime(time.Now())
	res.Headers["Date"] = currentTime
	if rqst.Close {
		res.Headers["Connection"] = "close"
	}
}

// 200 OK Response
func (res *Response) HandleOK(rqst *Request, path string) {
	res.StatusCode = 200
	res.StatusText = "OK"
	res.FilePath = path
	res.Request = rqst
	res.Proto = "HTTP/1.1"

	res.Headers = make(map[string]string)
	currentTime := time.Now()

	file, err := os.Stat(res.FilePath)
	if err != nil {
		fmt.Println("error when checking file stat")
	}
	modTime := file.ModTime()
	// update the headers using FormatTime in util.go
	res.Headers["Date"] = FormatTime(currentTime)
	res.Headers["Last-Modified"] = FormatTime(modTime)
	res.Headers["Content-Type"] = mime.TypeByExtension(filepath.Ext(rqst.URL))
	res.Headers["Content-Length"] = fmt.Sprintf("%v", int(file.Size()))

	if rqst.Close {
		res.Headers["Connection"] = "close"
	}

}
