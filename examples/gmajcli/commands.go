package main

import (
	"fmt"

	"github.com/r-medina/gmaj"
)

type command func(nodes []*gmaj.Node, args ...string) bool

var allCmds []string

var cmds = map[string]command{
	"quit": func(nodes []*gmaj.Node, args ...string) (stop bool) {
		fmt.Println("goodbye")

		stop = true
		return
	},

	"node": func(nodes []*gmaj.Node, args ...string) (stop bool) {
		for _, node := range nodes {
			fmt.Println(node)
		}
		return
	},

	"table": func(nodes []*gmaj.Node, args ...string) (stop bool) {
		for _, node := range nodes {
			fmt.Printf("%s: %s\n", gmaj.IDToString(node.Id), node.FingerTableString())
		}
		return
	},

	"addr": func(nodes []*gmaj.Node, args ...string) (stop bool) {
		for _, node := range nodes {
			fmt.Println(node.Addr)
		}
		return
	},

	"data": func(nodes []*gmaj.Node, args ...string) (stop bool) {
		for _, node := range nodes {
			fmt.Println(node.DatastoreString())
		}
		return
	},

	"get": func(nodes []*gmaj.Node, args ...string) (stop bool) {
		if len(args) > 0 {
			val, err := gmaj.Get(nodes[0], args[0])
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("%s\n", val)
			}
		}
		return
	},

	"put": func(nodes []*gmaj.Node, args ...string) (stop bool) {
		if len(args) > 1 {
			err := gmaj.Put(nodes[0], args[0], []byte(args[1]))
			if err != nil {
				fmt.Println(err)
			}
		}
		return
	},

	"help": func(nodes []*gmaj.Node, args ...string) (stop bool) {
		fmt.Printf("available commands: %v\n", allCmds)
		return
	},
}

func init() {
	allCmds = commands()
}

func commands() []string {
	out := make([]string, 0, len(cmds))
	for cmd := range cmds {
		out = append(out, cmd)
	}

	return out
}
