package main

import (
	"fmt"
	"os"

	"github.com/r-medina/gmaj"
	"github.com/r-medina/gmaj/gmajpb"

	"golang.org/x/net/context"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var config struct {
	parentAddr string
	client     gmajpb.GMajClient

	put struct {
		key string
		val string
	}

	get struct {
		key string
	}
}

var (
	app = kingpin.New("gmaj-client", "GMaj client").DefaultEnvars()
)

func init() {
	app.Flag("addr", "address of node to contact").StringVar(&config.parentAddr)

	put := app.Command("put", "Put a key").PreAction(getClient).Action(putKeyVal)
	put.Arg("key", "The key to put").StringVar(&config.put.key)
	put.Arg("value", "The key to put").StringVar(&config.put.val)

	get := app.Command("get", "Get a key").PreAction(getClient).Action(getKey)
	get.Arg("key", "The key to get").StringVar(&config.get.key)
}

func main() {
	if _, err := app.Parse(os.Args[1:]); err != nil {
		app.Fatalf("command line parsing failed: %v", err)
	}
}

func getClient(*kingpin.ParseContext) error {
	conn, err := gmaj.Dial(config.parentAddr)
	app.FatalIfError(err, "dialing parent %v failed: %v\n", config.parentAddr, err)

	config.client = gmajpb.NewGMajClient(conn)

	return nil
}

func putKeyVal(*kingpin.ParseContext) error {
	key := config.put.key
	val := config.put.val

	_, err := config.client.Put(context.Background(), &gmajpb.KeyVal{Key: key, Val: val})
	app.FatalIfError(err, "putting key %q value %q failed: %v\n", key, val, err)

	fmt.Println("put succeded")

	return nil
}

func getKey(*kingpin.ParseContext) error {
	key := config.get.key
	val, err := config.client.Get(context.Background(), &gmajpb.Key{Key: key})
	app.FatalIfError(err, "getting key %q failed: %v\n", key, err)

	fmt.Printf("%s: %s\n", key, val.Val)

	return nil
}
