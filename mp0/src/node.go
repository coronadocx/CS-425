package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"bufio"
	"log"
)

var l *log.Logger

func initLogger(nodename string) {
	tag := fmt.Sprintf("LOG (%s) : ", nodename)
	l = log.New(os.Stderr, tag, log.Lshortfile | log.Ltime)
}

func logWriteErr(err error) {
	l.Fatalln("Could not write to writter : ", err)
}

func logFlushErr(err error) {
	l.Fatalln("Could not flush writter : ", err)
}

func close(conn net.Conn, writer *bufio.Writer) {
	if err := writer.Flush(); err != nil {
		logFlushErr(err)
	}
	if 	_, err := writer.WriteString("nodeDisconnect"); err != nil {
		logWriteErr(err)
	}
	if err := writer.Flush(); err != nil {
		logFlushErr(err)
	}	
	if err := conn.Close(); err != nil {
		l.Println("Unable to close connection : ", err)
	}

	l.Println("Node closed successfully")
}

func initCloseHandler(conn net.Conn, writer *bufio.Writer) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func () {
		<-c
		close(conn, writer)
		os.Exit(0)
	} ()
}


func sendEvent(inputText string, writer *bufio.Writer) {
	if _,err := writer.WriteString(inputText + "\n"); err != nil {
		logWriteErr(err)
	}

	if err := writer.Flush(); err != nil {
		logFlushErr(err)
	}
}

func sendInitialEvent(nodename string, writer *bufio.Writer) {
	sendEvent(fmt.Sprintf("nodeConnect " + nodename), writer)
}


func main() {
	args := os.Args[1:]
	if len(args) != 3 {
		fmt.Fprintf(os.Stderr, "Have %v args, expected %v\n", len(args), 4)
		os.Exit(1) 
	}
	nodename  := args[0]
	ip_port   := fmt.Sprintf("%s:%s",args[1], args[2])
	conn, err := net.Dial("tcp", ip_port)
	if err != nil {
		fmt.Println("Could not establish connection to server")
		os.Exit(1)
	} 
	fmt.Fprintln(os.Stderr, "Server connection established!")

	scanner := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(conn)
	initCloseHandler(conn, writer)
	initLogger(nodename)
	sendInitialEvent(nodename, writer) 
	for {
		if scanner.Scan() {
			sendEvent(scanner.Text(), writer)
		}
	}
}