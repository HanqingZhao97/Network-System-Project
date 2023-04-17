package tritonhttp

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
)

const (
	OK         = 200
	NotFound   = 404
	BadRequest = 400
)

type Response struct {
	Proto      string // e.g. "HTTP/1.1"
	StatusCode int    // e.g. 200
	StatusText string // e.g. "OK"

	// Headers stores all headers to write to the response.
	Headers map[string]string

	// Request is the valid request that leads to this response.
	// It could be nil for responses not resulting from a valid request.
	// Hint: you might need this to handle the "Connection: Close" requirement
	Request *Request

	// FilePath is the local path to the file to serve.
	// It could be "", which means there is no file to serve.
	FilePath string
}

// Define the method to wrtie the response to writer
func (res *Response) Write(writer io.Writer) error {
	// Check write which part has error
	if err := res.WriteInitialLine(writer); err != nil {
		return err
	}
	if err := res.WriteHeaders(writer); err != nil {
		return err
	}
	if err := res.WriteBody(writer); err != nil {
		return err
	}
	return nil
}

func (res *Response) WriteInitialLine(writer io.Writer) error {
	bw := bufio.NewWriter(writer)
	initialLine := fmt.Sprintf("%v %v %v\r\n", res.Proto, res.StatusCode, res.StatusText)
	//write to buffer
	if _, err := bw.WriteString(initialLine); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return nil
}

func (res *Response) WriteHeaders(writer io.Writer) error {
	bw := bufio.NewWriter(writer)

	//sort the keys
	sortedKey := make([]string, 0)
	for key := range res.Headers {
		sortedKey = append(sortedKey, key)
	}
	sort.Strings(sortedKey)

	for _, key := range sortedKey {
		line := fmt.Sprintf("%v: %v\r\n", key, res.Headers[key])
		//write to buffer
		if _, err := bw.WriteString(line); err != nil {
			return err
		}
		if err := bw.Flush(); err != nil {
			return err
		}
	}
	//write the ending CRLF
	CRLF := "\r\n"
	if _, err := bw.WriteString(CRLF); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return nil
}

func (res *Response) WriteBody(w io.Writer) error {
	if len(res.FilePath) == 0 {
		return nil
	}

	content, err := os.ReadFile(res.FilePath)
	if err != nil {
		return err
	}

	bw := bufio.NewWriter(w)

	if _, err := bw.Write(content); err != nil {
		return err
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	return nil
}
