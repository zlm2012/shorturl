package shorturl

import (
	"database/sql"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"net/url"
	"time"
)

func NewManager(bk Backend) (*Manager, error) {
	snowflake.Epoch = 1657436936000 // 2022/7/10 7:8:56 UTC
	bkNodeId, err := bk.getNodeId()
	if err != nil {
		return nil, err
	}
	node, err := snowflake.NewNode(bkNodeId)
	if err != nil {
		return nil, err
	}
	return &Manager{node, bk}, nil
}

func (m *Manager) GetUrl(id snowflake.ID) string {
	return id.Base58()
}

func (m *Manager) InsertOrReuse(dstUrl string, expireAt int64) (snowflake.ID, error) {
	parsedDst, err := url.ParseRequestURI(dstUrl)
	if err != nil {
		return 0, err
	}
	if parsedDst.Scheme == "" {
		return 0, fmt.Errorf("not a valid dst url")
	}
	if expireAt > 0 && time.Now().Unix() > expireAt {
		return 0, fmt.Errorf("already expired")
	}
	existing, err := m.bk.QueryByUrl(dstUrl)
	if err != nil {
		return 0, err
	}
	for _, entry := range existing {
		if expireAt < 0 && !entry.ExpireAt.Valid {
			return snowflake.ID(entry.Id), nil
		}
		if expireAt > 0 && entry.ExpireAt.Valid && entry.ExpireAt.Int64 == expireAt {
			return snowflake.ID(entry.Id), nil
		}
	}
	id := m.snode.Generate()
	realExpireAt := sql.NullInt64{Int64: expireAt, Valid: expireAt > 0}
	err = m.bk.InsertUrl(&UrlEntry{Id: uint64(id), Url: dstUrl, ExpireAt: realExpireAt})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (m *Manager) Clean() error {
	return m.bk.ClearExpired()
}
