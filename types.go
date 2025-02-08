package shorturl

import (
	"database/sql"
	"github.com/bwmarrin/snowflake"
	"github.com/patrickmn/go-cache"
	"net/url"
)

type UrlEntry struct {
	Id       uint64
	Url      string
	ExpireAt sql.NullInt64
}

type Backend interface {
	InsertUrl(entry *UrlEntry) error
	QueryByUrl(url string) ([]UrlEntry, error)
	QueryById(id uint64) (*UrlEntry, error)
	ClearExpired() error
	Close() error
	getNodeId() (int64, error)
}

type Manager struct {
	snode *snowflake.Node
	bk    Backend
}

type Redirecter struct {
	bks     map[int64]Backend
	baseUrl *url.URL
	strict  bool
	cache   *cache.Cache
}
