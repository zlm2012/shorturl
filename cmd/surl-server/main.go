package main

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"log"
	"net/http"
	"shorturl"
)

var opts struct {
	Filenames []string `short:"f" long:"file" description:"path to sqlite3 db" required:"true"`
	BaseUrl   string   `short:"b" long:"base" description:"base url" required:"true"`
	Port      uint16   `short:"p" long:"port" description:"listen port" default:"8080"`
	Strict    bool     `long:"strict" description:"strict mode, checking host"`
}

func main() {
	_, err := flags.Parse(&opts)
	if err != nil {
		log.Fatalln(err)
	}
	redirecter, err := shorturl.NewRedirecter(opts.Filenames, opts.BaseUrl, opts.Strict)
	if err != nil {
		log.Fatalln(err)
	}
	log.Fatalln(http.ListenAndServe(fmt.Sprintf(":%v", opts.Port), redirecter))
}
