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
	isPrimary bool
	kvDB      map[string]string
}

func (server *KVServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	// if primary is updating the backup
	if args.FromPrimary {
		server.kvDB[args.Key] = args.Value
		reply.Err = OK
		// if request is sent to primary
	} else if server.isPrimary {
		reply.PreviousValue = server.kvDB[args.Key]
		server.kvDB[args.Key] = args.Value
		if args.DoHash {
			h := hash(reply.PreviousValue + args.Value)
			server.kvDB[args.Key] = strconv.Itoa(int(h))
		}
		if server.view.Backup != "" {
			// RPC arguments
			putArgs := &PutArgs{}
			putArgs.Key = args.Key
			putArgs.Value = args.Value
			putArgs.FromPrimary = true
			var putReply PutReply
			// keep going until both the RPC call is correct and the reply is ok
			valueFetched := false
			for putReply.Err != OK {
				// update the value for the backup
				valueFetched = call(server.view.Backup, "KVServer.Put", putArgs, &putReply)
				fmt.Println(server.view.Backup, valueFetched)
			}
		}
		// value replicated successfuly
		reply.Err = OK
		// ignore the other cases
	} else {
		reply.Err = ErrWrongServer
		fmt.Println("wrrrrrrrrrrrrong server")
	}
	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	if server.isPrimary {
		reply.Value = server.kvDB[args.Key]
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}
	return nil
}

func (server *KVServer) Update(args *UpdateArgs, reply *UpdateReply) error {
	server.kvDB = args.KVDB
	reply.Err = OK
	fmt.Println("inside update", server.isPrimary)
	return nil
}

// ping the viewserver periodically.
func (server *KVServer) tick() {

	// This line will give an error initially as view and err are not used.
	view, err := server.monitorClnt.Ping(server.view.Viewnum)

	// Your code here.
	// handle error
	if err != nil {
		fmt.Println("exit with error")
		return
	}
	// if primary, it can handle the client's requests
	// fmt.Println("server.id:", server.id, "server.view.Primary:", server.view.Primary, "view.Primary:", view.Primary)
	if server.id == view.Primary {
		// fmt.Println("it's read")
		server.isPrimary = true
	}
	// if a new backup is detected, then give it an updated version of the DB
	if server.view.Backup != view.Backup && view.Backup != "" {
		fmt.Println("this is a huge deal !!!")
		// RPC arguments
		args := &UpdateArgs{}
		args.KVDB = server.kvDB
		var reply UpdateReply

		call(view.Backup, "KVServer.Update", args, &reply)
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
	server.isPrimary = false
	server.kvDB = make(map[string]string)
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
