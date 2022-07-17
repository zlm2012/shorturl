package main

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"log"
	"shorturl"
	"time"
)

var opts struct {
	Filename string `short:"f" long:"file" description:"path to sqlite3 db" required:"true"`
	NodeId   int64  `short:"n" long:"node" description:"node id for snowflake" default:"1"`
	ExpireIn int64  `short:"e" long:"expire" description:"expire in (seconds)" default:"-1"`
}

func main() {
	args, err := flags.Parse(&opts)
	if err != nil {
		log.Fatalln(err)
	}
	bk, err := shorturl.SqliteOpen(opts.Filename, true, opts.NodeId)
	if err != nil {
		log.Fatalln(err)
	}
	defer func(bk shorturl.Backend) {
		_ = bk.Close()
	}(bk)
	mgr, err := shorturl.NewManager(bk)
	if err != nil {
		log.Fatalln(err)
	}
	expireAt := int64(-1)
	if opts.ExpireIn > 0 {
		expireAt = time.Now().Unix() + opts.ExpireIn
	}
	switch args[0] {
	case "add":
		insert(mgr, args[1], expireAt)
	case "clean":
		if err = mgr.Clean(); err != nil {
			log.Fatalln("failed on cleaning:", err)
		}
	}
}

func insert(mgr *shorturl.Manager, url string, expireAt int64) {
	id, err := mgr.InsertOrReuse(url, expireAt)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(mgr.GetUrl(id))
}
