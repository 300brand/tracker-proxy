package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

var (
	addr     = flag.String("addr", env("PROXY_ADDR", ":9294"), "Listen address for HTTP requests")
	logstash = flag.String("logstash", env("PROXY_LOGSTASH", "127.0.0.1:9293"), "Input TCP address for logstash")
)

var (
	Success      = new(uint64)
	ConnectError = new(uint64)
	CopyError    = new(uint64)
	Total        = new(uint64)
)

func main() {
	flag.Parse()

	go PrintStats(time.Hour)

	http.HandleFunc("/_status", HandleStatus)
	http.HandleFunc("/", HandleInbound)

	fmt.Printf("I Listening on %s\n", *addr)
	http.ListenAndServe(*addr, nil)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func HandleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	fmt.Fprint(w, Stats())
}

func HandleInbound(w http.ResponseWriter, r *http.Request) {
	defer atomic.AddUint64(Total, 1)
	defer r.Body.Close()

	conn, err := net.Dial("tcp", *logstash)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		fmt.Printf("E Failed to connect to %s: %s\n", *logstash, err)
		atomic.AddUint64(ConnectError, 1)
		return
	}
	defer conn.Close()

	n, err := io.Copy(conn, r.Body)
	if err != nil {
		fmt.Printf("E io.Copy: %s\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		atomic.AddUint64(CopyError, 1)
		return
	}

	atomic.AddUint64(Success, 1)
	w.Header().Add("X-Bytes-Written", fmt.Sprint(n))
}

func PrintStats(freq time.Duration) {
	for {
		<-time.After(freq)
		fmt.Println("S " + Stats())
	}
}

func Stats() string {
	return fmt.Sprintf(`{ "now": "%s", success": %d, "connect_error": %d, "copy_error": %d, "total": %d }`+"\n",
		time.Now().Format(time.RFC3339),
		atomic.LoadUint64(Success),
		atomic.LoadUint64(ConnectError),
		atomic.LoadUint64(CopyError),
		atomic.LoadUint64(Total),
	)
}
