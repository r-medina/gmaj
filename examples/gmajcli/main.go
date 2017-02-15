package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"

	"github.com/r-medina/gmaj"
	"github.com/r-medina/gmaj/gmajpb"
)

const prompt = "quit|node|table|addr|data|get|put > "

// nodeToString takes a gmaj.Node and generates a short semi-descriptive string.
func nodeToString(node *gmaj.Node) string {
	var succ []byte
	var pred []byte
	if node.Successor != nil {
		succ = node.Successor.Id
	}
	if node.Predecessor != nil {
		pred = node.Predecessor.Id
	}

	return fmt.Sprintf(
		"Node-%v: {succ:%v, pred:%v}", gmaj.IDToString(node.Id), succ, pred,
	)
}

func main() {
	countPtr := flag.Int(
		"count", 1, "Total number of Chord nodes to start up in this process",
	)
	addrPtr := flag.String(
		"addr", "", "Address of a node in the Chord ring you wish to join",
	)
	idPtr := flag.String(
		"id", "", "ID of a node in the Chord ring you wish to join",
	)

	flag.Parse()

	var parent *gmajpb.Node
	if *addrPtr == "" {
		parent = nil
	} else {
		val := big.NewInt(0)
		val.SetString(*idPtr, 10)
		parent = &gmajpb.Node{
			Id:   val.Bytes(),
			Addr: *addrPtr,
		}
		fmt.Printf(
			"Attach this node to id:%v, addr:%v\n",
			gmaj.IDToString(parent.Id), parent.Addr,
		)
	}

	nodes := make([]*gmaj.Node, *countPtr)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c

		for _, node := range nodes {
			node.Shutdown()
		}

		os.Exit(1)
	}()

	var err error
	for i := range nodes {
		nodes[i], err = gmaj.NewNode(parent)
		if err != nil {
			fmt.Println("Unable to create new node!")
			log.Fatal(err)
		}

		fmt.Printf(
			"Created -id %v -addr %v\n",
			gmaj.IDToString(nodes[i].Id), nodes[i].Addr,
		)
	}

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print(prompt)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		args := strings.SplitN(line, " ", 3)

		switch args[0] {
		case "node":
			for _, node := range nodes {
				fmt.Println(nodeToString(node))
			}
		case "table":
			for _, node := range nodes {
				fmt.Println(gmaj.FingerTableToString(node))
			}
		case "addr":
			for _, node := range nodes {
				fmt.Println(node.Addr)
			}
		case "data":
			for _, node := range nodes {
				gmaj.PrintDataStore(node)
			}
		case "get":
			if len(args) > 1 {
				val, err := gmaj.Get(nodes[0], args[1])
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println(val)
				}
			}
		case "put":
			if len(args) > 2 {
				err := gmaj.Put(nodes[0], args[1], args[2])
				if err != nil {
					fmt.Println(err)
				}
			}
		case "quit":
			fmt.Println("goodbye")
			for _, node := range nodes {
				node.Shutdown()
			}
			return
		}

		fmt.Print(prompt)
	}
}
