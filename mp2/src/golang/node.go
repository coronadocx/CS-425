/*
Stuff that will run in the background:
	new connection listener
	introduction handler
	transatcion handler

Stuff that will be called (non-thread)
	initial introduction
	dead connection 

What information does a node need to start?
	How to connect to the introduction server
	What port to operate on (to allow for multiple nodes on the same IP)

*/

package main

import (
	"os"
	"net"
	"fmt"
	"time"
	"sync"
	"bufio"
	"bytes"
	"strconv"
	"strings"
	"math/rand"
	"encoding/gob"
	"crypto/sha256"
	"container/list" 
)

// Globals 
var (
	VERBOSE int = 0
	all_ip = [10]string {
		"172.22.156.169", 
        "172.22.158.169",
        "172.22.94.168",
        "172.22.156.170",
        "172.22.158.170",
        "172.22.94.169",
        "172.22.156.171",
        "172.22.158.171",
        "172.22.94.170",
        "172.22.156.172",
	}

	intro_ip  string = all_ip[0]	// Assume server runs on VM1
	service_conn net.Conn 			// Global Connection to service
	self_name string
	self_ip   string
	self_port string

	neighbors map[string]Node = make(map[string]Node)	// Dict of connections, accessed by name
	n_mutex   sync.Mutex

	balances map[string]int = make(map[string]int)		// Dict to keep track of balances. This is only for committed transactions
	b_mutex  sync.Mutex

	spec_bal map[string]int = make(map[string]int)		// Dict to keep track of non-committed balances. This is used to determine if transactions are valid
	sb_mutex sync.Mutex

	block_chain = list.New()
	bc_mutex sync.Mutex 

	acc_trans []string
	at_mutex sync.Mutex

	trans_chan   chan string  = make(chan string, 100)	// Buffered channels for dispatching messages
	intro_chan   chan string  = make(chan string, 100)
	neigh_chan   chan string  = make(chan string, 100)
	solved_chan  chan string  = make(chan string, 100) 
	catchup_chan chan string  = make(chan string, 100)
	verify_chan  chan string  = make(chan string, 100)
	block_chan   chan Block   = make(chan Block, 100)
	abandon_chan chan int     = make(chan int, 100) 
	chain_chan   chan []Block = make(chan []Block)

	msg_to_log   chan string  = make(chan string, 1024)
	band_to_log  chan string  = make(chan string, 1024)
	flag_for_chain_split chan string = make(chan string, 1024)


	trans_queue     []string 	// Slice acting as a queue. Transactions in queue are already seen and not retransmitted
	trans_queue_max int = 1024	// When to start removing elements from the queue

	block_queue		[]Block
	block_queue_max int = 32

	then = time.Date(2020, 4, 15, 20, 34, 58, 651387237, time.UTC)

)

type Node struct {
	name 		string
	ip   		string
	port 		string
	conn 		net.Conn
}

type Block struct {
	Soln_hash   string
	Prev_hash   string
	Name        string
	Trans  	  []string
	Height      int 
	Bal 		map[string]int
}

type ProtoBlock struct {
	Prev_hash   string
	Trans     []string 
}


/**
 * Listen to a specific node for any new messages
 * Dispatch new messages to their corresponding channels for further processing
 **/
func nodeListener(n Node) {
	c := n.conn
	r := bufio.NewReader(c)
	// timeout = 10 * time.Second()

	for {
		// Read msg
		// c.SetReadDeadline(time.Now().Add(timeout))	// Pings should be received every 5 seconds
		msg,err := r.ReadString('\n')
		if err != nil {
			fmt.Printf("(%s)#:Failure to read %s : %s\n",self_name, n.name, err)
			// Connection closed, remove from neighbors
			n_mutex.Lock()
			delete(neighbors, n.name)
			n_mutex.Unlock()
			return
		}


        // put message into log band
        band_to_log <- msg
		// Parse msg
		// fmt.Print("#:",msg)
		msg_fields := strings.Fields(msg)
		switch (msg_fields[0]){
		case "INTRODUCE":
			intro_chan <- msg
		case "TRANSACTION":
			trans_chan <- msg
		case "NEIGHBORS":
			neigh_chan <- msg
		case "BLOCK":
			printMsg("New block")
			var res []byte
			len,_ := strconv.Atoi(msg_fields[1])
			for i:=0; i<len; i++ {
				tmp,_ := r.ReadByte()
				res = append(res,tmp)
			}
			tmpBuf := bytes.NewBuffer(res)
			dec := gob.NewDecoder(tmpBuf)
			tmpBlk := Block{}
			dec.Decode(&tmpBlk)	
			block_chan <- tmpBlk
		case "CATCHUP":
			printMsg("New catch-up")
			catchup_chan <- msg
		case "CHAIN":
			printMsg("New chain") 
			var res []byte
			len,_ := strconv.Atoi(msg_fields[1])
			for i:=0; i<len; i++ {
				tmp,_ := r.ReadByte()
				res = append(res,tmp)
			}
			tmpBuf := bytes.NewBuffer(res)
			dec := gob.NewDecoder(tmpBuf)
			var new_block_chain []Block
			dec.Decode(&new_block_chain)
			chain_chan <- new_block_chain
		default:
			fmt.Printf("(%s)#:Unknown message (nodeListener) : %s\n",self_name,msg)
			os.Exit(1)
		}
	}
}

/**
 * Listen for other nodes attempting to connect
 * On new connection:
 * 		Make a new entry in the neighbors map
 *		Launch a nodeListener instance
 **/
func introListener() {
	listener,err := net.Listen("tcp",fmt.Sprintf("%s:%s",self_ip,self_port))
	if err != nil {
		fmt.Printf("(%s)#:Listen Err :  %s\n", err)
		os.Exit(1)
	}
	for {
		// Establish connection
		conn,_     := listener.Accept()
		r          := bufio.NewReader(conn)
		msg,_      := r.ReadString('\n')
		msg_fields := strings.Fields(msg)
		if _,is_in := neighbors[msg_fields[0]]; is_in {
			fmt.Printf("(%s)#:Double Connection %s %s\n", self_name, self_name, msg_fields[0]) // Might not need this
		}

		// Append to map
		new_node := Node {
			msg_fields[0],
			msg_fields[1],
			msg_fields[2],
			conn,
		}
		n_mutex.Lock()
		neighbors[new_node.name] = new_node
		n_mutex.Unlock()

		// Start listening for messages
		go nodeListener(new_node)
	}
}

/**
 * Responsible for reacting to INTRODUCE messagess
 * Takes input from intro_chan
 * Attempts to connect to new neighbors when less than 20 are known
 * Stores resulting connections into the global neighbors map
 **/
func introHandler() {
	for {
		next_conn := strings.Fields(<-intro_chan)[1:]
		if len(neighbors) >= 20 {
			continue
		} else if _,is_in := neighbors[next_conn[0]]; is_in {
			continue
		} else if next_conn[0] == self_name {
			continue
		}

		// Attempt connection
		ip_port := fmt.Sprintf("%s:%s",next_conn[1],next_conn[2])
		conn, err := net.Dial("tcp",ip_port)
		if err != nil {
			fmt.Println(err)
			fmt.Printf("(%s)#:Could not connect to %s %s %s\n",self_name, next_conn[0], next_conn[1], next_conn[2])
			continue
		}

		// On success, send self info to other node
		fmt.Fprintf(conn, fmt.Sprintf("%s %s %s\n", self_name, self_ip, self_port))

		new_node := Node {
			next_conn[0],
			next_conn[1],
			next_conn[2],
			conn,	
		}

		n_mutex.Lock()
		neighbors[new_node.name] = new_node
		n_mutex.Unlock()

		go nodeListener(new_node)
	}
}


/**
 * Responsible for reacting to TRANSACTION messages
 * Takes inpuf from trans_chan
 * Check to see if a given transaction is valid.
 * First makes sure this isn't a repeat transaction
 * Then checks to see if the resulting balances are correct
 * If everything looks good, the new balances are saved and the transaction is marked as valid
 * Propagates TRANSACTION to neighbors
 **/
func transHandler() {
	for {
		// Check if transaction is valid
		msg := <-trans_chan
		msg = strings.TrimSuffix(msg, "\n")
		if inQ := inQueue(msg); inQ {
			continue
		} else {
			enQueue(msg)	// Add to ignore queue
		}

		// log messages
		msg_to_log <- msg

		msg_fields := strings.Fields(msg)[1:]
		src := msg_fields[2]
		dst := msg_fields[3] 
		amt,_ := strconv.Atoi(msg_fields[4])

		// Repropagate to other nodes, doesn't matter if valid for this node
		n_mutex.Lock()
		for _,node := range neighbors {
			fmt.Fprintf(node.conn, "%s\n",msg)
		}
		n_mutex.Unlock()

		// Check balances
		sb_mutex.Lock()
		src_bal := 999999999
		if src != "0" {
			src_bal = spec_bal[src]	// If no entry exists, default to 0 which is fine
		}
		dst_bal := spec_bal[dst]	// If no entry exists, default to 0 which is fine
		sb_mutex.Unlock()
		if src_bal < amt {
			printMsg(fmt.Sprintf("REJ %s",msg))
			continue
		}

		/////////////////////LISHEN START HERE///////////////////////////

		// Update speculative balances
		sb_mutex.Lock()
		if src != "0" {
			spec_bal[src] = src_bal - amt
		}
			spec_bal[dst] = dst_bal + amt
		sb_mutex.Unlock()


		printMsg(msg)
		at_mutex.Lock()
		acc_trans = append(acc_trans,msg)
		at_mutex.Unlock()
	}
}

// Very basic brute-force search
func inQueue(msg string) bool {
	for _,trans := range trans_queue {
		if msg == trans {
			return true
		}
	}

	return false
}

/**
 * Put transaction into transaction queue
 * Remove elements if necessary
 **/
func enQueue(msg string) {
	trans_queue = append(trans_queue, msg)
	if len(trans_queue) > trans_queue_max {
		trans_queue = trans_queue[1:]
	}
}


/**
 * Responsible for reacting to NEIGHBORS messages
 * Takes input from neigh_chan
 * Replies with INTRODUCE messages for each node in neighbors
 **/
func neighHandler() {
	for {
		msg := strings.Fields(<-neigh_chan)[1:]
		n_mutex.Lock()
		neighbor,is_in := neighbors[msg[0]]
		n_mutex.Unlock()
		if !is_in {
			fmt.Print("Unknown connection : ",msg)
			continue
		}

		// Reply with random selection of neighbors
		n_mutex.Lock()
		for _,node := range neighbors {
			if rand.Intn(100) < 75 {
				fmt.Fprintf(neighbor.conn, "INTRODUCE %s %s %s\n", node.name, node.ip, node.port)
			}
		}
		n_mutex.Unlock()
	}
}

// Periodically send messages to neighbors asking for their neighbors. 
func pollForIntros() {
	time.Sleep(2*time.Second)		// Delay a few seconds to allow initial setup
	for {
		time.Sleep(10*time.Second)	// Not really sure how often this should happen
		n_mutex.Lock()
		for _,node := range neighbors {
			fmt.Fprintf(node.conn, "NEIGHBORS %s\n",self_name)
		}
		n_mutex.Unlock()
	}
}


/**
 * On get CATCHUP message,
 * Put block_chain into a slice
 * Encode the slice
 * Send off the CHAIN reply with the new block_chain info
 **/
func catchupHandler() {
	for {
		msg := <-catchup_chan
		msg_fields := strings.Fields(msg)[1:]
		n_mutex.Lock()
		replyConn := neighbors[msg_fields[0]]
		n_mutex.Unlock()
		var bcBuf bytes.Buffer
		var bc     []Block
		enc := gob.NewEncoder(&bcBuf)
		bc_mutex.Lock()
		for cur:=block_chain.Front();cur!=nil;cur=cur.Next() {
			bc = append(bc,(cur.Value.(Block)))
		}
		bc_mutex.Unlock()
		enc.Encode(&bc)
		reply := fmt.Sprintf("CHAIN %d\n",bcBuf.Len())
		replyBuf := bytes.NewBufferString(reply)
		replyBuf.Write(bcBuf.Bytes())
		replyConn.conn.Write(replyBuf.Bytes())
	}
}


/** 
 * Responsible for reacting to BLOCK messages
 * Takes input from block_chan
 * On get BLOCK
 *   Ignore 
 *   Validate and replace
 **/
func blockHandler() {
	for {
		new_block := <- block_chan
		// Determine if the new block should be repropagated
		if inQ := inBlockQueue(new_block); inQ {
			continue
		} else {
			enBlockQueue(new_block)	// Add to ignore queue
		}

		// Convert block struct to message
		var out_bytes bytes.Buffer
		out_enc := gob.NewEncoder(&out_bytes)
		err := out_enc.Encode(&new_block)
		if err != nil {
			fmt.Printf("(%s)#:Error in blockHandler : Propagate: %s\n",self_name, err)
		}
		outgoing := fmt.Sprintf("BLOCK %d\n", out_bytes.Len())
		msg_b := bytes.NewBufferString(outgoing)
		msg_b.Write(out_bytes.Bytes())
		
		// Repropagate to other nodes
		n_mutex.Lock()
		for _,node := range neighbors {
			node.conn.Write(msg_b.Bytes())
		}
		n_mutex.Unlock()



		// Determine if the new block should be accpeted
		if valid := isBlockValid(new_block); !valid {
			fmt.Printf("(%s)#:Error in blockHandler : Invalid Block\n%v\n",self_name, new_block)
			continue 	// Ignore invalid blocks
		}

		bc_mutex.Lock()
		prev_hash := getPrevHash()	// Needs to be in bc_mutex

		switch {
		case new_block.Height <= block_chain.Len():
			printMsg("Ignoring new block")

		case new_block.Height == (block_chain.Len()+1):
			switch {
			case new_block.Prev_hash == prev_hash:
				printMsg("Accepting new block...")
				abandonBuild(new_block.Name, new_block.Height)
				block_chain.PushFront(new_block)
				updateBalances(new_block)
				pruneAccTrans(1)
				printMsg("Accepted")
			case new_block.Prev_hash != prev_hash: 
				// log split
				flag_for_chain_split <- "dummy"

				printMsg("Catching-up to new block...")
				abandonBuild(new_block.Name, new_block.Height)
				requestCatchup(new_block.Name)
				updateBalances(new_block)
				pruneAccTrans(1)
				bc_mutex.Unlock()
				printMsg("All caught-up")			
			}

		case new_block.Height > (block_chain.Len()+1):
			// log split
			flag_for_chain_split <- "dummy"

			printMsg("Catching-up to new block...")
			abandonBuild(new_block.Name, new_block.Height)
			diff := new_block.Height - block_chain.Len()
			requestCatchup(new_block.Name)
			updateBalances(new_block)
			pruneAccTrans(diff)
			bc_mutex.Unlock()
			printMsg("All caught-up")
		}

		bc_mutex.Unlock()
	}
}

func updateBalances(b Block) {
	b_mutex.Lock()
	sb_mutex.Lock()
	balances = b.Bal
	spec_bal = b.Bal
	sb_mutex.Lock()
	b_mutex.Unlock()
}

// Very basic brute-force search
func inBlockQueue(b Block) bool {
	for _,blk := range block_queue {
		if b.Soln_hash == blk.Soln_hash {
			if b.Prev_hash == blk.Prev_hash {
				return true
			}
		}
	}

	return false
}

/**
 * Put transaction into transaction queue
 * Remove elements if necessary
 **/
func enBlockQueue(b Block) {
	block_queue = append(block_queue, b)
	if len(block_queue) > block_queue_max {
		block_queue = block_queue[1:]
	}
}

/**
 * Helper, used to check if a given block is valid
 **/
func isBlockValid(b Block) bool {
	var b_serial bytes.Buffer
	b_encoder := gob.NewEncoder(&b_serial)
	err := b_encoder.Encode(ProtoBlock{b.Prev_hash, b.Trans})
	if err != nil {
		fmt.Printf("(%s)#:Error in isBlockValid Encode :%s\n",self_name, err)
	}
	b_hash := fmt.Sprintf("%x",sha256.Sum256(b_serial.Bytes()))		// Compute hash of prev + trans
	fmt.Fprintf(service_conn, "VERIFY %s %s\n", b_hash, b.Soln_hash)	// Send hash and soln to be verified
	if resp := <- verify_chan; resp == "OK" {						// return results
		return true
	} else {
		return false
	}
}

/**
 * Helper, tell the builder not to commit to a block
 **/
func abandonBuild(name string, h int) {
	if name != self_name {
		abandon_chan <- h
	}
}

/** Needs bc_mutex
 * Helper, ask the block sender for it's blockchain
 * Use the new blockchain to replace current blockchain
 **/
func requestCatchup(origin string) {
	n_mutex.Lock()
	dest := neighbors[origin]
	n_mutex.Unlock()
	fmt.Fprintf(dest.conn, "CHAIN %s\n", self_name)
	reply := <- chain_chan
	new_block_chain := list.New()
	for _,block := range reply {
		new_block_chain.PushFront(block)
	}
	block_chain = new_block_chain
}

/** TODO 
 * Helper, go through the first diff blocks and remove duplicate transactions
 * This runs in o(nnn) soooo hopefully we don't need this too often 
 **/
func pruneAccTrans(diff int) {
	at_mutex.Lock()
	cur := block_chain.Front()
	for i:=0; i<diff; i++ {
		cur_block := (cur.Value.(Block))
		for _,bTrans := range cur_block.Trans {
			for idx,trans := range acc_trans {
				if bTrans == trans {
					copy(acc_trans[idx:], acc_trans[idx+1:]) 
					acc_trans[len(acc_trans)-1] = "" 
					acc_trans = acc_trans[:len(acc_trans)-1]     
				}
			}
		}
	}
	at_mutex.Unlock()
}


/** NOTE Change timeout values before testing
 * Assemble blocks based off of current transactions
 * Will begin assembly whichever is first:
 *     2000 transactions arrive
 *     1 minute has passed
 **/
func blockBuilder() {
	timeout := time.Now().Add(time.Duration(10+rand.Intn(10))*time.Second)
	for {
		at_mutex.Lock()
		cur_trans := acc_trans
		at_mutex.Unlock()

		if len(cur_trans) > 2000 {
			cur_trans = cur_trans[:1999]
		}

		if time.Now().After(timeout) || len(cur_trans) >= 2000 {
			bc_mutex.Lock()
			bc_height := block_chain.Len()
			bc_mutex.Unlock() 
			printMsg("Building block")
			proto_hash := getProtoHash(cur_trans)
			fmt.Fprintf(service_conn, "SOLVE %s\n", proto_hash) 

			select {
			case msg := <- solved_chan:
				msg_fields := strings.Fields(msg)
				if msg_fields[1] == proto_hash {
					printMsg("Got solution hash, propagating block")
					propagateBlock(msg_fields[2], cur_trans)
					timeout = time.Now().Add(time.Duration(10+rand.Intn(10))*time.Second)

					logMsgInBlock(cur_trans)
				} else {
					printMsg("Got old solution hash, ignoring")
				}
			case abandon_height := <-abandon_chan:
				// Abandon requests are the height of the block that generates it
				// We ignore old abandon requests by not resetting the timeout value
				if abandon_height > bc_height {
					timeout = time.Now().Add(time.Duration(10+rand.Intn(10))*time.Second)
					printMsg("Abandoning Block")
				}
			}	
		}
		time.Sleep(100*time.Microsecond)
	}
}

func getProtoHash(cur_trans []string) string {
	var pBlock_buffer bytes.Buffer 
	pBlock_encoder := gob.NewEncoder(&pBlock_buffer)
	bc_mutex.Lock()
	err := pBlock_encoder.Encode(ProtoBlock{getPrevHash(),cur_trans})
	bc_mutex.Unlock()
	if err != nil {
		fmt.Printf("(%s)#:Error in getProtoHash Encode :%s\n",self_name, err)
	}

	return fmt.Sprintf("%x",sha256.Sum256(pBlock_buffer.Bytes()))
}

// Need to sandwich these in bc_mutex
func getPrevHash() string {
	if block_chain.Len() == 0 {
		return "0"
	} else { 
		prev := block_chain.Front()
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(&prev)
		return fmt.Sprintf("%x",sha256.Sum256(buf.Bytes()))	// Generate hash to send in for solving
	}
}

func propagateBlock(soln string, cur_trans []string) {
	bc_mutex.Lock()
	defer bc_mutex.Unlock()

	sb_mutex.Lock()
	outgoing_block := Block {
		Soln_hash:soln,
		Prev_hash:getPrevHash(),
		Trans:cur_trans,
		Height: (block_chain.Len()+1),
		Name: self_name,
		Bal: spec_bal,
	}
	sb_mutex.Unlock()

	// Add block to block_chain
	block_chain.PushFront(outgoing_block)

	// Update balances
	sb_mutex.Lock()
	b_mutex.Lock()
	balances = spec_bal
	b_mutex.Unlock()
	sb_mutex.Unlock()


	// Update transactions
	at_mutex.Lock()
	acc_trans = acc_trans[(len(cur_trans)):]
	at_mutex.Unlock()

	// Convert block struct to message
	var out_bytes bytes.Buffer
	out_enc := gob.NewEncoder(&out_bytes)
	err := out_enc.Encode(&outgoing_block)
	if err != nil {
		fmt.Printf("(%s)#:Error in blockBuilder Propagate: %s\n",self_name, err)
	}
	outgoing := fmt.Sprintf("BLOCK %d\n", out_bytes.Len())
	msg_b := bytes.NewBufferString(outgoing)
	msg_b.Write(out_bytes.Bytes())
	
	// Repropagate to other nodes
	enBlockQueue(outgoing_block)	// Add to ignore queue
	n_mutex.Lock()
	for _,node := range neighbors {
		node.conn.Write(msg_b.Bytes())
	}
	n_mutex.Unlock()
}


func printMsg(msg string) {
	if VERBOSE == 1 {
		fmt.Printf("(%s)#:%s\n",self_name,msg)
	}
} 

// Log new messages
// accepts chan from msg_to_log
func logMsg() {
	// create file
	// when multiple nodes initiated for one VM
	// their self_names are different. see .sh file
	f, err := os.Create("transOutput/" + self_name + "out.txt")
	check(err)
	defer f.Close()
	// log messages from msg_to_log
	var s string
	for {
		s = <- msg_to_log
		now := time.Now()
		diff := now.Sub(then)

		t := fmt.Sprintf("%f", diff.Seconds())
		s = s + " " + t + "\n"
		f.WriteString(s)
	}
}

func logMsgInBlock(trans []string) {
	// create file
	// when multiple nodes initiated for one VM
	// their self_names are different. see .sh file
	f, err := os.Create("transInBlockOutput/" + self_name + "out.txt")
	check(err)
	defer f.Close()
	// log transactions
	for _, s := range trans {
		now := time.Now()
		diff := now.Sub(then)

		t := fmt.Sprintf("%f", diff.Seconds())
		s = s + " " + t + "\n"
		f.WriteString(s)
	}
}

func logChainSplit() {
	f, err := os.Create("splitOutput/" + self_name + "out.txt")
	check(err)
	defer f.Close()
	for {
		<- flag_for_chain_split
		now := time.Now()
		diff := now.Sub(then)

		t := fmt.Sprintf("%f", diff.Seconds())
		t = t + "\n"
		f.WriteString(t)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func logBand() {
	f, err := os.Create("bandOutput/" + self_name + "out.txt")
	check(err)
	defer f.Close()

	var s string
	for {
		s = <- band_to_log
		length := strconv.Itoa(len(s))
		now := time.Now()
		diff := now.Sub(then)

		t := fmt.Sprintf("%f", diff.Seconds())
		f.WriteString(t + " " + length + "\n")
	}
}

/*
// For debugging
// Print a node's neighbors every 5 seconds
func printConnections() {
	for {
		time.Sleep(5*time.Second)
		n_mutex.Lock()
		fmt.Println(self_name, " connections: ", neighbors)
		n_mutex.Unlock()
	}
}




// Log connections
func logConn() {

}
*/


func main() {
	args := os.Args[1:]
	if len(args) < 4 || len(args) > 5{
		fmt.Println("usage: ./node <SELF_NAME> <SELF_VM> <SELF_PORT> <INTRO_PORT> <VERBOSE=0>")
		os.Exit(1)
	}

	if len(args) == 5 {
		VERBOSE,_ = strconv.Atoi(args[4])
	} else {
		VERBOSE = 0
	}

	// Parse args
	self_vm,_ := strconv.Atoi(args[1])

	self_name   = args[0]
	self_ip     = all_ip[(self_vm-1)]
	self_port   = args[2]
	intro_port := args[3]

	// Launch Threads
	// go printConnections()	
	go introListener()		// Listen for new connections
	go introHandler()		// Act on any introdction messages
	go transHandler()		// Act on transactios
	go neighHandler()
	go catchupHandler()
	go blockHandler()
	if VERBOSE == 1 {
		go blockBuilder()
	}
	go pollForIntros()
	go logMsg()
	// go logBand()


	// Connect to introduction service 
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s",intro_ip,intro_port))
	if err != nil {
		fmt.Println("Could not connect to introduction service", err)
		os.Exit(1)
	}

	service_conn = conn

	// Send initial connect msg
	intro_scanner := bufio.NewScanner(conn)
	fmt.Fprintf(conn, "CONNECT %s %s %s\n", self_name, self_ip, self_port)
	time.Sleep(time.Second)		// Might be able to reduce this length, need to experiment

	// Listen for messages from introduction service, dispatch accordingly
	for {
		if intro_scanner.Scan() {
			msg :=  intro_scanner.Text()
			msg_fields := strings.Fields(msg)
			switch (msg_fields[0]) {
			case "INTRODUCE": 
				intro_chan <- msg
			case "TRANSACTION": 
				trans_chan <- msg
			case "SOLVED":
				solved_chan <- msg
			case "VERIFY":
				verify_chan <- msg_fields[1]
			case "QUIT":	// Graceful Termination
				os.Exit(1)
			case "DIE":		// Hard Termination
				os.Exit(1)
			default:
				fmt.Println("Unknown message (main) : ",msg)
				os.Exit(1)
			}
		}
	}
}
