package kvservice

import (
	"asg4/sysmonitor"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type KVServer struct {
	l           net.Listener
	dead        bool // for testing
	unreliable  bool // for testing
	id          string
	monitorClnt *sysmonitor.Client
	view        sysmonitor.View
	done        sync.WaitGroup
	finish      chan interface{}

	// Add your declarations here.
	isPrimary         bool
	kvDB              map[string]string
	getClientRequests map[string]GetReply
	putClientRequests map[string]PutReply
	mutex             sync.RWMutex
}

func (server *KVServer) PutBackup(args *PutArgs, reply *PutReply) error {
	if server.isPrimary {
		reply.Err = ErrWrongServer
		server.isPrimary = false
		return nil
	}
	server.isPrimary = false
	// filter duplicate requests
	if server.putClientRequests[args.RequestId].Err == OK {
		*reply = server.putClientRequests[args.RequestId]
		return nil
	}
	server.updatePutHash(args, reply)
	return nil
}

func (server *KVServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.isPrimary {
		if server.view.Backup != "" {
			// RPC PUT arguments
			putArgs := &PutArgs{}
			putArgs.Key = args.Key
			putArgs.Value = args.Value
			putArgs.DoHash = args.DoHash
			putArgs.RequestId = args.RequestId
			var putReply PutReply
			// keep going until the reply is ok
			for putReply.Err != OK {
				// update the value for the backup
				ok := call(server.view.Backup, "KVServer.PutBackup", putArgs, &putReply)
				// if RPC call failed, try again later
				if !ok {
					// atomicLock.Unlock()
					return nil
				}
				if putReply.Err == ErrWrongServer {
					server.isPrimary = false
					reply.Err = ErrWrongServer
					return nil
				}
			}
		}
		// filter duplicate requests
		if server.putClientRequests[args.RequestId].Err == OK {
			*reply = server.putClientRequests[args.RequestId]
			return nil
		}
		server.updatePutHash(args, reply)
		// ignore the other cases
	} else {
		reply.Err = ErrWrongServer
	}
	return nil
}

func (server *KVServer) GetBackup(args *GetArgs, reply *GetReply) error {
	if server.isPrimary {
		reply.Err = ErrWrongServer
		server.isPrimary = false
		return nil
	}
	server.isPrimary = false
	// filter duplicate requests
	if server.getClientRequests[args.RequestId].Err == OK {
		*reply = server.getClientRequests[args.RequestId]
		return nil
	}
	server.updateGet(args, reply)
	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.isPrimary {
		if server.view.Backup != "" {
			// RPC GET arguments
			getArgs := &GetArgs{}
			getArgs.Key = args.Key
			getArgs.RequestId = args.RequestId
			var getReply GetReply
			// keep going until the reply is ok
			for getReply.Err != OK {
				// update the value for the backup
				ok := call(server.view.Backup, "KVServer.GetBackup", getArgs, &getReply)
				// if RPC call failed, try again later
				if !ok {
					return nil
				}
				if getReply.Err == ErrWrongServer {
					server.isPrimary = false
					reply.Err = ErrWrongServer
					return nil
				}
			}
		}
		// filter duplicate requests
		if server.getClientRequests[args.RequestId].Err == OK {
			*reply = server.getClientRequests[args.RequestId]
			return nil
		}
		server.updateGet(args, reply)
	} else {
		reply.Err = ErrWrongServer
	}
	return nil
}

func (server *KVServer) Update(args *UpdateArgs, reply *UpdateReply) error {
	if server.isPrimary {
		reply.Err = ErrWrongServer
		server.isPrimary = false
		return nil
	}
	server.kvDB = args.KVDB
	server.isPrimary = false
	server.getClientRequests = args.GetClientRequests
	server.putClientRequests = args.PutClientRequests
	reply.Err = OK
	return nil
}

// ping the viewserver periodically.
func (server *KVServer) tick() {
	// This line will give an error initially as view and err are not used.
	view, err := server.monitorClnt.Ping(server.view.Viewnum)

	// Your code here.
	server.mutex.Lock()
	defer server.mutex.Unlock()
	// handle error
	if err != nil {
		return
	}
	// if primary, it can handle the client's requests
	if server.id == view.Primary {
		server.isPrimary = true
	} else {
		server.isPrimary = false
	}
	// if a new backup is detected, then give it an updated version of the DB
	// if server.isPrimary && server.view.Backup != view.Backup && view.Backup != "" {
	if server.isPrimary && view.Backup != "" {
		// RPC UPDATE arguments
		args := &UpdateArgs{}
		args.KVDB = server.kvDB
		args.GetClientRequests = server.getClientRequests
		args.PutClientRequests = server.putClientRequests
		var reply UpdateReply
		for reply.Err != OK {
			ok := call(view.Backup, "KVServer.Update", args, &reply)
			if !ok {
				server.view = view
				return
			}
			if reply.Err == ErrWrongServer {
				server.isPrimary = false
				server.view = view
				return
			}
		}
	}
	server.view = view
}

// tell the server to shut itself down.
// please do not change this function.
func (server *KVServer) Kill() {
	server.dead = true
	server.l.Close()
}

func StartKVServer(monitorServer string, id string) *KVServer {
	server := new(KVServer)
	server.id = id
	server.monitorClnt = sysmonitor.MakeClient(id, monitorServer)
	server.view = sysmonitor.View{}
	server.finish = make(chan interface{})

	// Add your server initializations here
	// ==================================
	server.getClientRequests = make(map[string]GetReply)
	server.putClientRequests = make(map[string]PutReply)
	server.isPrimary = false
	server.kvDB = make(map[string]string)
	server.mutex = sync.RWMutex{}
	//====================================

	rpcs := rpc.NewServer()
	rpcs.Register(server)

	os.Remove(server.id)
	l, e := net.Listen("unix", server.id)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	server.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for server.dead == false {
			conn, err := server.l.Accept()
			if err == nil && server.dead == false {
				if server.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if server.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				} else {
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && server.dead == false {
				fmt.Printf("KVServer(%v) accept: %v\n", id, err.Error())
				server.Kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", server.id)
		server.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(server.finish)
	}()

	server.done.Add(1)
	go func() {
		for server.dead == false {
			server.tick()
			time.Sleep(sysmonitor.PingInterval)
		}
		server.done.Done()
	}()

	return server
}

func (server *KVServer) updatePutHash(args *PutArgs, reply *PutReply) {
	reply.PreviousValue = server.kvDB[args.Key]
	server.kvDB[args.Key] = args.Value
	if args.DoHash {
		h := hash(reply.PreviousValue + args.Value)
		server.kvDB[args.Key] = strconv.Itoa(int(h))
	}
	// value replicated successfuly
	reply.Err = OK
	// prevent any more requests
	server.putClientRequests[args.RequestId] = *reply
}

func (server *KVServer) updateGet(args *GetArgs, reply *GetReply) {
	reply.Value = server.kvDB[args.Key]
	reply.Err = OK
	// prevent any more requests
	server.getClientRequests[args.RequestId] = *reply
}
