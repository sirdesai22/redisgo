// main.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	ADDRESS   = "127.0.0.1:6380"
	AOF_FILE  = "appendonly.aof"
	SNAPSHOT  = "dump.rdb"
	AOF_SYNC  = true // set false to buffer/faster but less durable
	EXPIRY_TICK = 1 * time.Second
)

type entry struct {
	Value     []byte
	ExpiresAt time.Time // zero means no expiry
}

type DB struct {
	mu    sync.RWMutex
	data  map[string]*entry
	aof   *os.File
	aofMu sync.Mutex
}

func NewDB() (*DB, error) {
	f, err := os.OpenFile(AOF_FILE, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	db := &DB{
		data: make(map[string]*entry),
		aof:  f,
	}
	if err := db.loadAOF(); err != nil {
		return nil, err
	}
	go db.expiryWorker()
	return db, nil
}

// Basic commands
func (db *DB) Set(key string, value []byte, ttl time.Duration) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	ent := &entry{Value: value}
	if ttl > 0 {
		ent.ExpiresAt = time.Now().Add(ttl)
	}
	db.data[key] = ent
	return db.appendAOF("SET", key, value, ttl)
}

func (db *DB) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	e, ok := db.data[key]
	if !ok || ( !e.ExpiresAt.IsZero() && time.Now().After(e.ExpiresAt)) {
		return nil, false
	}
	return e.Value, true
}

func (db *DB) Del(keys ...string) int {
	db.mu.Lock()
	defer db.mu.Unlock()
	deleted := 0
	for _, k := range keys {
		if _, ok := db.data[k]; ok {
			delete(db.data, k)
			deleted++
			// optionally record DEL in AOF
			_ = db.appendAOF("DEL", k, nil, 0)
		}
	}
	return deleted
}

func (db *DB) appendAOF(cmd, key string, value []byte, ttl time.Duration) error {
	db.aofMu.Lock()
	defer db.aofMu.Unlock()
	// Store AOF as JSON lines for simplicity: {"cmd":"SET","key":"k","value":"base64","ttl_ms":123}
	rec := map[string]interface{}{
		"cmd": cmd,
		"key": key,
	}
	if value != nil {
		rec["value"] = string(value) // value is raw bytes; for binary, use base64
	}
	if ttl > 0 {
		rec["ttl_ms"] = int64(ttl / time.Millisecond)
	}
	b, _ := json.Marshal(rec)
	if _, err := db.aof.Write(append(b, '\n')); err != nil {
		return err
	}
	if AOF_SYNC {
		return db.aof.Sync()
	}
	return nil
}

func (db *DB) loadAOF() error {
	db.aofMu.Lock()
	defer db.aofMu.Unlock()
	_, err := db.aof.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(db.aof)
	for scanner.Scan() {
		line := scanner.Bytes()
		var rec map[string]interface{}
		if err := json.Unmarshal(line, &rec); err != nil {
			continue // skip invalid line
		}
		cmd := strings.ToUpper(fmt.Sprintf("%v", rec["cmd"]))
		key := fmt.Sprintf("%v", rec["key"])
		switch cmd {
		case "SET":
			val := []byte(fmt.Sprintf("%v", rec["value"]))
			var ttl time.Duration
			if t, ok := rec["ttl_ms"]; ok {
				if ms, ok2 := t.(float64); ok2 && ms > 0 {
					ttl = time.Duration(int64(ms)) * time.Millisecond
				}
			}
			db.data[key] = &entry{Value: val}
			if ttl > 0 {
				db.data[key].ExpiresAt = time.Now().Add(ttl)
			}
		case "DEL":
			delete(db.data, key)
		}
	}
	return nil
}

func (db *DB) expiryWorker() {
	t := time.NewTicker(EXPIRY_TICK)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		db.mu.Lock()
		for k, e := range db.data {
			if !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt) {
				delete(db.data, k)
			}
		}
		db.mu.Unlock()
	}
}

// Snapshotting (simple)
func (db *DB) SaveSnapshot() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	tmp := make(map[string]map[string]interface{})
	for k, e := range db.data {
		ent := map[string]interface{}{
			"value":     string(e.Value),
			"expiresAt": e.ExpiresAt.UnixNano(),
		}
		tmp[k] = ent
	}
	b, err := json.MarshalIndent(tmp, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(SNAPSHOT, b, 0644)
}

// TCP server and simple text protocol: commands are lines like:
// SET key value [PX milliseconds]
// GET key
// DEL key [key...]
// SAVE
// PING
func handleConn(conn net.Conn, db *DB) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		line, err := r.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				// fmt.Println("read err:", err)
			}
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		args := splitArgs(line)
		if len(args) == 0 {
			io.WriteString(conn, "-ERR empty command\r\n")
			continue
		}
		switch strings.ToUpper(args[0]) {
		case "PING":
			io.WriteString(conn, "+PONG\r\n")
		case "SET":
			if len(args) < 3 {
				io.WriteString(conn, "-ERR wrong number of args for SET\r\n")
				continue
			}
			key := args[1]
			val := []byte(args[2])
			var ttl time.Duration
			if len(args) >= 5 && strings.ToUpper(args[3]) == "PX" {
				if ms, err := time.ParseDuration(args[4] + "ms"); err == nil {
					ttl = ms
				}
			}
			if err := db.Set(key, val, ttl); err != nil {
				io.WriteString(conn, "-ERR "+err.Error()+"\r\n")
			} else {
				io.WriteString(conn, "+OK\r\n")
			}
		case "GET":
			if len(args) != 2 {
				io.WriteString(conn, "-ERR wrong number of args for GET\r\n")
				continue
			}
			if v, ok := db.Get(args[1]); ok {
				io.WriteString(conn, fmt.Sprintf("$%d\r\n%s\r\n", len(v), string(v)))
			} else {
				io.WriteString(conn, "$-1\r\n")
			}
		case "DEL":
			if len(args) < 2 {
				io.WriteString(conn, "-ERR wrong number of args for DEL\r\n")
				continue
			}
			removed := db.Del(args[1:]...)
			io.WriteString(conn, fmt.Sprintf(":%d\r\n", removed))
		case "SAVE":
			if err := db.SaveSnapshot(); err != nil {
				io.WriteString(conn, "-ERR "+err.Error()+"\r\n")
			} else {
				io.WriteString(conn, "+OK\r\n")
			}
		default:
			io.WriteString(conn, "-ERR unknown command\r\n")
		}
	}
}

func splitArgs(line string) []string {
	// very simple splitter; does not support quoted spaces.
	parts := strings.Fields(line)
	return parts
}

func main() {
	db, err := NewDB()
	if err != nil {
		fmt.Println("failed to start db:", err)
		return
	}
	ln, err := net.Listen("tcp", ADDRESS)
	if err != nil {
		fmt.Println("listen error:", err)
		return
	}
	fmt.Println("Listening on", ADDRESS)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("accept err:", err)
			continue
		}
		go handleConn(conn, db)
	}
}
