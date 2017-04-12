package main

import (
	"fmt"
	"io/ioutil"
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

	put := app.Command("put", "put a key - if value argument is missing, reads from stdin").
		PreAction(getClient).Action(putKeyVal)
	put.Arg("key", "the key to put").StringVar(&config.put.key)
	put.Arg("value", "the key to put").StringVar(&config.put.val)

	get := app.Command("get", "get a key").PreAction(getClient).Action(getKey)
	get.Arg("key", "the key to get").StringVar(&config.get.key)
}

func main() {
	if _, err := app.Parse(os.Args[1:]); err != nil {
		app.FatalUsage("command line parsing failed: %v", err)
	}
}

func getClient(*kingpin.ParseContext) error {
	conn, err := gmaj.Dial(config.parentAddr)
	app.FatalIfError(err, "dialing parent %v failed", config.parentAddr)

	config.client = gmajpb.NewGMajClient(conn)

	return nil
}

func putKeyVal(*kingpin.ParseContext) error {
	key := config.put.key
	strVal := config.put.val
	var val []byte

	if strVal == "" {
		var err error
		val, err = ioutil.ReadAll(os.Stdin)
		app.FatalIfError(err, "failed to read from stdin")
	} else {
		val = []byte(strVal)
	}

	_, err := config.client.Put(context.Background(), &gmajpb.PutRequest{
		Key: key, Value: val,
	})
	app.FatalIfError(err, "putting key %q failed", key)

	fmt.Println("put succeded")

	return nil
}

func getKey(*kingpin.ParseContext) error {
	key := config.get.key
	resp, err := config.client.Get(context.Background(), &gmajpb.GetRequest{Key: key})
	app.FatalIfError(err, "getting key %q failed", key)

	fmt.Printf("%s", resp.Value)

	return nil
}
