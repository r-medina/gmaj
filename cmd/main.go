package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"

	pb "github.com/r-medina/gmaj"
	"github.com/r-medina/gmaj/gmaj"
)

func NodeStr(node *gmaj.Node) string {
	var succ []byte
	var pred []byte
	if node.Successor != nil {
		succ = node.Successor.Id
	}
	if node.Predecessor != nil {
		pred = node.Predecessor.Id
	}

	return fmt.Sprintf("Node-%v: {succ:%v, pred:%v}", node.Id, succ, pred)
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

	var parent *pb.RemoteNode
	if *addrPtr == "" {
		parent = nil
	} else {
		parent = new(pb.RemoteNode)
		val := big.NewInt(0)
		val.SetString(*idPtr, 10)
		parent.Id = val.Bytes()
		parent.Addr = *addrPtr
		fmt.Printf("Attach this node to id:%v, addr:%v\n", parent.Id, parent.Addr)
	}

	var err error
	nodes := make([]*gmaj.Node, *countPtr)
	for i := range nodes {
		nodes[i], err = gmaj.NewNode(parent)
		if err != nil {
			fmt.Println("Unable to create new node!")
			log.Fatal(err)
		}

		if parent == nil {
			parent = nodes[i].RemoteNode
		}
		fmt.Printf(
			"Created -id %v -addr %v\n",
			gmaj.IDToString(nodes[i].Id), nodes[i].Addr,
		)
	}

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("quit|node|table|addr|data|get|put > ")
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		args := strings.SplitN(line, " ", 3)

		switch args[0] {
		case "node":
			for _, node := range nodes {
				fmt.Println(NodeStr(node))
			}
		case "table":
			for _, node := range nodes {
				gmaj.PrintFingerTable(node)
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
		default:
			continue
		}

		fmt.Printf("quit|node|table|addr|data|get|put > ")
	}
}
