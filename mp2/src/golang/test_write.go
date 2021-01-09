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
	"fmt"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	var name string = "node_1_"
	f, err :=os.Create(name + "out.txt")
	check(err)

	defer f.Close()
    var s string
    s = "I am a string!"
	n3, err := f.WriteString("writes a string \n" + s)
	fmt.Printf("wrote %d bytes\n", n3)

	f.Sync()

}
