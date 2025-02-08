package shorturl

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/fsnotify/fsnotify"
	"github.com/patrickmn/go-cache"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

func NewRedirecter(files []string, baseUrl string, strict bool, enableCache bool) (*Redirecter, error) {
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

	var urlCache *cache.Cache = nil
	if enableCache {
		urlCache = createCacheWithFileWatcher(files)
	}
	return &Redirecter{bks, realBaseUrl, strict, urlCache}, nil
}

func createCacheWithFileWatcher(files []string) *cache.Cache {
	urlCache := cache.New(5*time.Minute, 10*time.Minute)

	// fsnotify for urlCache clear
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Println("fsnotify init failed, just ignore.", err)
	} else {
		go func() {
			for {
				select {
				case event, ok := <-watcher.Events:
					if !ok {
						return
					}
					if event.Has(fsnotify.Write) {
						urlCache.Flush() // clear cache if DB modified
					}
				case err, ok := <-watcher.Errors:
					if !ok {
						return
					}
					log.Println("fsnotify error:", err)
				}
			}
		}()

		for _, f := range files {
			err = watcher.Add(f)
			if err != nil {
				log.Printf("failed on watching %v, ignored: %v", f, err)
			}
		}
	}

	return urlCache
}

func (r *Redirecter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		w.WriteHeader(404)
		_, _ = io.WriteString(w, "not found")
		return
	}
	if r.strict && req.Host != r.baseUrl.Host {
		w.WriteHeader(404)
		_, _ = io.WriteString(w, "not found")
		return
	}
	reqPathDir, reqFinalSeg := path.Split(req.URL.Path)
	if reqPathDir != r.baseUrl.Path {
		w.WriteHeader(404)
		_, _ = io.WriteString(w, "not found")
		return
	}
	if reqFinalSeg == "" {
		w.WriteHeader(404)
		_, _ = io.WriteString(w, "not found")
		return
	}
	if r.cache != nil {
		if cachedUrl, found := r.cache.Get(reqFinalSeg); found {
			if cachedUrl == nil {
				w.WriteHeader(404)
				_, _ = io.WriteString(w, "not found")
				return
			}
			http.Redirect(w, req, cachedUrl.(string), 302)
		}
	}
	id, err := snowflake.ParseBase58([]byte(reqFinalSeg))
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
		log.Printf("failed on querying %s: %v", reqFinalSeg, err)
		w.WriteHeader(500)
		_, _ = io.WriteString(w, "temporarily error")
		return
	}
	if entry == nil {
		// cache
		if r.cache != nil {
			r.cache.Add(reqFinalSeg, nil, cache.DefaultExpiration)
		}

		w.WriteHeader(404)
		_, _ = io.WriteString(w, "not found")
		return
	}

	//cache
	if r.cache != nil {
		if !entry.ExpireAt.Valid {
			r.cache.Set(reqFinalSeg, entry.Url, cache.DefaultExpiration)
		} else {
			r.cache.Set(reqFinalSeg, entry.Url, time.Unix(entry.ExpireAt.Int64, 0).Sub(time.Now()))
		}
	}
	http.Redirect(w, req, entry.Url, 302)
}
