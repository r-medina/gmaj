package main

import (
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"

	"github.com/Bowery/prompt"
	"github.com/r-medina/gmaj"
	"github.com/r-medina/gmaj/gmajpb"
)

const promptStr = "gmaj> "

func main() {
	count := flag.Int(
		"count", 1, "Total number of Chord nodes to start up in this process",
	)
	parentAddr := flag.String(
		"parent-addr", "", "Address of a node in the Chord ring you wish to join",
	)
	parentID := flag.String(
		"parent-id", "", "ID of a node in the Chord ring you wish to join",
	)
	addr := flag.String(
		"addr", "", "Address to listen on",
	)

	flag.Parse()

	var parent *gmajpb.Node
	if *parentAddr == "" {
		parent = nil
	} else {
		val := big.NewInt(0)
		val.SetString(*parentID, 10)
		parent = &gmajpb.Node{
			Id:   val.Bytes(),
			Addr: *parentAddr,
		}
		fmt.Printf(
			"Attach this node to id:%v, addr:%v\n",
			gmaj.IDToString(parent.Id), parent.Addr,
		)
	}

	nodes := make([]*gmaj.Node, *count)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c

		shutdown(nodes)

		os.Exit(1)
	}()

	var err error
	for i := range nodes {
		nodes[i], err = gmaj.NewNode(parent, gmaj.WithAddress(*addr))
		if err != nil {
			fmt.Println("Unable to create new node!")
			log.Fatal(err)
		}
		parent = nodes[i].Node

		fmt.Printf(
			"Created -id %v -addr %v\n",
			gmaj.IDToString(nodes[i].Id), nodes[i].Addr,
		)
	}

	cmds["help"](nil)
loop:
	for {
		args := []string{}
		line, err := prompt.Basic(promptStr, false)
		if err != nil {
			if err == prompt.ErrEOF {
				args = append(args, "quit")
			} else {
				continue
			}
		} else {
			args = strings.Fields(line)
			if len(args) == 0 {
				args = append(args, "")
			}
		}

		cmd, ok := cmds[args[0]]
		if !ok {
			continue
		}

		var stop bool
		if len(args) > 1 {
			stop = cmd(nodes, args[1:]...)
		} else {
			stop = cmd(nodes)
		}

		if stop {
			break loop
		}
	}

	shutdown(nodes)
}

func shutdown(nodes []*gmaj.Node) {
	fmt.Println("shutting down...")

	for _, node := range nodes {
		node.Shutdown()
	}
}
