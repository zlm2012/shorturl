package shorturl

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"time"
)

type sqliteBackend struct {
	db      *sql.DB
	version int64
}

func SqliteOpen(filename string, isWrite bool, nodeId int64) (*sqliteBackend, error) {
	if isWrite && (nodeId < 0 || nodeId > 1023) {
		return nil, fmt.Errorf("%v is not a valid snowflake node id", nodeId)
	}
	_, err := os.Stat(filename)
	needInit := false
	if os.IsNotExist(err) && isWrite {
		file, err := os.Create(filename)
		if err != nil {
			return nil, err
		}
		_ = file.Close()
		needInit = true
	} else if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}
	if needInit {
		_, err = db.Exec(fmt.Sprintf("PRAGMA user_version = %d", nodeId), nil)
		if err != nil {
			return nil, err
		}
		err = createTables(db)
		if err != nil {
			return nil, err
		}
	}
	s := &sqliteBackend{db, 0}
	dbNodeId, err := s.getNodeId()
	if err != nil {
		return nil, err
	}
	if isWrite && dbNodeId != nodeId {
		return nil, fmt.Errorf("node id is not identical, expected %d, actually got %d", dbNodeId, nodeId)
	}
	return s, nil
}

func createTables(db *sql.DB) error {
	ddl := `CREATE TABLE url (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"url" TEXT NOT NULL,
			"expire_at" INTEGER);`
	stmt, err := db.Prepare(ddl)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	return err
}

func (s *sqliteBackend) InsertUrl(entry *UrlEntry) error {
	query := `INSERT INTO url(id, url, expire_at) VALUES (?,?,?)`
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(entry.Id, entry.Url, entry.ExpireAt)
	return err
}

func (s *sqliteBackend) QueryByUrl(url string) ([]UrlEntry, error) {
	query := `SELECT * FROM url WHERE url = ? AND (expire_at IS NULL OR expire_at > ?)`
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	row, err := stmt.Query(url, now)
	if err != nil {
		return nil, err
	}
	defer func(row *sql.Rows) {
		_ = row.Close()
	}(row)
	result := make([]UrlEntry, 0)
	for row.Next() {
		var entry UrlEntry
		err = row.Scan(&entry.Id, &entry.Url, &entry.ExpireAt)
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, nil
}

func (s *sqliteBackend) QueryById(id uint64) (*UrlEntry, error) {
	query := `SELECT * FROM url WHERE id = ? AND (expire_at IS NULL OR expire_at > ?)`
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	row, err := stmt.Query(id, now)
	if err != nil {
		return nil, err
	}
	defer func(row *sql.Rows) {
		_ = row.Close()
	}(row)
	for row.Next() {
		var entry UrlEntry
		err = row.Scan(&entry.Id, &entry.Url, &entry.ExpireAt)
		if err != nil {
			return nil, err
		}
		return &entry, nil
	}
	return nil, nil
}

func (s *sqliteBackend) ClearExpired() error {
	_, _ = s.db.Exec(`DROP TABLE tmp_url`)
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err = createTmpTables(s.db); err != nil {
		return err
	}

	rows, err := tx.Query(`SELECT * FROM url WHERE expire_at IS NULL OR expire_at > ?`, time.Now().Unix())
	var entry UrlEntry
	for rows.Next() {
		err = rows.Scan(&entry.Id, &entry.Url, &entry.ExpireAt)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO tmp_url(id, url, expire_at) VALUES (?,?,?)`, entry.Id, entry.Url, entry.ExpireAt)
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(`DROP TABLE url`)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`ALTER TABLE tmp_url RENAME TO url`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func createTmpTables(db *sql.DB) error {
	ddl := `CREATE TABLE tmp_url (
			"id" INTEGER NOT NULL PRIMARY KEY,
			"url" TEXT NOT NULL,
			"expire_at" INTEGER);`
	stmt, err := db.Prepare(ddl)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	return err
}

func (s *sqliteBackend) Close() error {
	return s.db.Close()
}

func (s *sqliteBackend) getNodeId() (int64, error) {
	row, err := s.db.Query(`PRAGMA user_version`)
	if err != nil {
		return 0, err
	}
	defer func(row *sql.Rows) {
		_ = row.Close()
	}(row)
	for row.Next() {
		var userVer int64
		err = row.Scan(&userVer)
		if err != nil {
			return 0, err
		}
		return userVer & 0x3f, nil
	}
	return 0, nil
}
