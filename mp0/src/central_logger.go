package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	// "syscall"
	"bufio"
	"time"
	"strings"
	"strconv"
	// "log"
	"sync"
)

// var l *log.Logger
var delaySlice []float64
var bandwidthSlice []int
var mutex = &sync.Mutex{}

func close(listener net.Listener) {
	listener.Close()
}

func initCloseHandler(listener net.Listener) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func () {
		<-c
		close(listener)
		os.Exit(0)
	} ()
}

func getTime() float64 {
	return float64(time.Now().UnixNano())/1e9
}

func recordStats(msg string) {
	msgFields := strings.Fields(msg)
	msgTime,_ := strconv.ParseFloat(msgFields[0], 64)
	msgDelay  := getTime() - msgTime
	msgSize   := len(msg)

	mutex.Lock()
	delaySlice = append(delaySlice,msgDelay)
	bandwidthSlice = append(bandwidthSlice,msgSize)
	mutex.Unlock()
}

func statLogger() {
	var numDelays, bwAvg int
	var delays string
	for {
		time.Sleep(time.Second)
		bwAvg = 0
		mutex.Lock()
		for _,bandWidth := range bandwidthSlice {		// Calc avg bandwidth
			bwAvg += bandWidth
		}

		numDelays = 0
		delays = ""
		for _,delay := range delaySlice {
			numDelays++
			delays += fmt.Sprintf("%v,",delay)
		}
		mutex.Unlock()
		delaySlice = nil
		bandwidthSlice = nil
		if numDelays == 0 {
			fmt.Fprintf(os.Stderr, "%d,%d,%s\n",0, numDelays, delays)
		} else {
			fmt.Fprintf(os.Stderr, "%d,%d,%s\n",bwAvg, numDelays, delays)
		}
	}
}

func handleNode(node net.Conn) () {	
	var msgFields []string
	var nodeName string
	var startTime float64 = getTime()

	s := bufio.NewScanner(node)
	for {
		if s.Scan() {
			msg := s.Text()
			msgFields = strings.Fields(msg)
			switch msgFields[0] {
			case "nodeConnect":
				nodeName = msgFields[1]
				fmt.Printf("%f - %v connected\n", startTime, nodeName)

			case "nodeDisconnect":
				fmt.Printf("%f - %v disconnected\n", getTime(), nodeName)
                                return

			default:  
				recordStats(msg)
				fmt.Printf("%v %v %v\n", msgFields[0], nodeName, msgFields[1])
			}
		}
	}
}


func main() {
	args := os.Args[1:]
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Have %v args, expected %v\n", len(args), 2)
		os.Exit(1)
	}
	
	server_port := fmt.Sprintf(":%s", args[0])
	listener,_ := net.Listen("tcp", server_port)
	//initCloseHandler(listener)
	go statLogger()

	for {
		node,_ := listener.Accept()	// Should block until next connection
		go handleNode(node)
	}

}
