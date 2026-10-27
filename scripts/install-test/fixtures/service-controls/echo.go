// Command echo is the workload of the chart's service-controls live test. It
// serves a TCP echo on :50060 from an image without Python, as the shipped Go
// service images are. With -dial host:port it instead sends one message to
// that address and exits non-zero unless the same bytes come back.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

const message = "chart-service-ok"

func main() {
	dial := flag.String("dial", "", "address to send one message to and check its echo")
	flag.Parse()
	if *dial != "" {
		if err := check(*dial); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := serve(":50060"); err != nil {
		log.Fatal(err)
	}
}

func serve(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer conn.Close()
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			_, _ = conn.Write(buf[:n])
		}()
	}
}

func check(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	if _, err := conn.Write([]byte(message)); err != nil {
		return err
	}
	got := make([]byte, len(message))
	if _, err := io.ReadFull(conn, got); err != nil {
		return err
	}
	if !bytes.Equal(got, []byte(message)) {
		return fmt.Errorf("echo from %s: got %q, want %q", addr, got, message)
	}
	return nil
}
