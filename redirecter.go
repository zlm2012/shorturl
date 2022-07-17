package shorturl

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
)

func NewRedirecter(files []string, baseUrl string) (*Redirecter, error) {
	bks := make(map[int64]Backend)
	for _, f := range files {
		bk, err := SqliteOpen(f, false, 0)
		if err != nil {
			return nil, err
		}
		nodeId, err := bk.getNodeId()
		if err != nil {
			return nil, err
		}
		if _, ok := bks[nodeId]; ok {
			return nil, fmt.Errorf("duplicated nodeIds in files provided on node ID %d", nodeId)
		}
		bks[nodeId] = bk
	}
	realBaseUrl, err := url.ParseRequestURI(baseUrl)
	if err != nil {
		return nil, err
	}
	if realBaseUrl.Scheme == "" {
		return nil, fmt.Errorf("baseUrl is not a full url")
	}
	if !strings.HasSuffix(realBaseUrl.Path, "/") {
		realBaseUrl.Path += "/"
	}
	return &Redirecter{bks, realBaseUrl}, nil
}

func (r *Redirecter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	reqPathDir, reqBase := path.Split(req.RequestURI)
	if reqPathDir != r.baseUrl.Path {
		w.WriteHeader(404)
		_, _ = io.WriteString(w, "not found")
		return
	}
	id, err := snowflake.ParseBase58([]byte(reqBase))
	if err != nil {
		w.WriteHeader(404)
		_, _ = io.WriteString(w, "not found")
		return
	}
	if _, ok := r.bks[id.Node()]; !ok {
		w.WriteHeader(404)
		_, _ = io.WriteString(w, "not found")
		return
	}
	entry, err := r.bks[id.Node()].QueryById(uint64(id))
	if err != nil {
		log.Printf("failed on querying %s: %v", reqBase, err)
		w.WriteHeader(500)
		_, _ = io.WriteString(w, "temporarily error")
		return
	}
	if entry == nil {
		w.WriteHeader(404)
		_, _ = io.WriteString(w, "not found")
		return
	}
	http.Redirect(w, req, entry.Url, 302)
}
