package tritonhttp

import (
	"bufio"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type Request struct {
	Method string // e.g. "GET"
	URL    string // e.g. "/path/to/a/file"
	Proto  string // e.g. "HTTP/1.1"

	// Headers stores the key-value HTTP headers
	Headers map[string]string

	Host  string // determine from the "Host" header
	Close bool   // determine from the "Connection" header
}

func ReadRequest(reader *bufio.Reader) (rqst *Request, received bool, err error) {
	rqst = &Request{}
	rqst.Headers = make(map[string]string)

	//start line
	line, err := ReadLines(reader)
	if err != nil {
		return nil, false, err
	}

	//headers
	rqst.Method, rqst.URL, rqst.Proto, err = ParseInitial(line)

	if err != nil {
		return nil, false, errors.New("wrong read start line: " + line)
	}
	//Check the requirements
	if rqst.Method != "GET" {
		return nil, false, errors.New("Method is not GET: " + rqst.Method)
	}
	if rqst.URL[:1] != "/" {
		return nil, false, errors.New("URL format is not correct: " + rqst.URL)
	}
	if rqst.Proto != "HTTP/1.1" {
		return nil, false, errors.New("HTTP version incorrect: " + rqst.Proto)
	}

	// Check if the request has host
	missing_host := true

	for {
		line, err := ReadLines(reader)
		if err != nil {
			return nil, true, err
		}
		if line == "" {
			break
		}
		if strings.Contains(line, "Host") {
			missing_host = false
		}

		err = rqst.ParseHeader(line)
		if err != nil {
			return nil, true, err
		}

		//check the request
		fmt.Println("Read from request: ", line)
	}

	//Handle if no Host shown
	if missing_host {
		return nil, true, errors.New("No host exist, 400: " + rqst.Host)
	}
	return rqst, true, nil
}

func ReadLines(reader *bufio.Reader) (string, error) {
	var line string
	for {
		//delimeter as \n
		str, err := reader.ReadString('\n')
		line += str
		if err != nil {
			return line, err
		}
		//when reaching end
		if strings.HasSuffix(line, "\r\n") {
			line = line[:len(line)-2]
			return line, nil
		}
	}
}

// function to split the initial line by space
func ParseInitial(line string) (string, string, string, error) {
	splits := strings.SplitN(line, " ", -1)
	if len(splits) != 3 {
		return "", "", "", fmt.Errorf("error parse initial line, got %v", splits)
	}
	return splits[0], splits[1], splits[2], nil
}

// functino to split the header part of the request
func (rqst *Request) ParseHeader(line string) error {
	splits := strings.SplitN(line, ": ", -1)
	if len(splits) != 2 {
		return fmt.Errorf("invalid request format")
	}
	//get keys and values
	key := CanonicalHeaderKey(splits[0])
	value := strings.TrimSpace(splits[1])

	//see if the format is correct
	if !regexp.MustCompile(`^[a-zA-z0-9-]+$`).MatchString(key) {
		return fmt.Errorf("key format wrong")
	}
	//assign value
	if key == "Host" {
		rqst.Host = value
	} else if key == "Connection" && value == "close" {
		rqst.Close = true
	} else {
		rqst.Headers[key] = value
	}
	return nil

}
