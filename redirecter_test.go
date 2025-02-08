package shorturl

import (
	"database/sql"
	"github.com/bwmarrin/snowflake"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-"

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func deleteId(files []string, idStr string, t *testing.T) {
	id, err := snowflake.ParseBase58([]byte(idStr))
	if err != nil {
		t.Fatal("wrong id.", err)
	}
	bk, err := SqliteOpen(files[id.Node()], true, id.Node())
	if err != nil {
		t.Fatal("failed on creating db.", err)
	}
	err = bk.Delete(uint64(id))
	if err != nil {
		t.Fatal("failed on delete id.", err)
	}
}

func initTest(dbCount int, entryCount int, t testing.TB) ([]string, map[string]string, map[string]interface{}, int64, []string) {
	snowflake.Epoch = 1657436936000 // 2022/7/10 7:8:56 UTC
	var files []string
	var nonexists []string
	expiringId := make(map[string]interface{})
	idUrlMap := make(map[string]string)
	expiredAt := time.Now().Unix() + 2
	for i := 0; i < dbCount; i++ {
		node, err := snowflake.NewNode(int64(i))
		if err != nil {
			t.Fatal("failed on creating snowflake node.", err)
		}

		f, err := os.CreateTemp(t.TempDir(), "rdb-")
		if err != nil {
			t.Fatal("failed on creating file", err)
		}
		if err = os.Remove(f.Name()); err != nil {
			t.Fatal("failed on deleting temp file for name", err)
		}
		bk, err := SqliteOpen(f.Name(), true, int64(i))
		if err != nil {
			t.Fatal("failed on creating db", err)
		}

		for j := 0; j < entryCount; j++ {
			id := node.Generate()
			url := "https://example.mrzm.io/" + randStr(18)
			sqlExpiredAt := sql.NullInt64{Valid: false}
			if j%5 == 0 {
				sqlExpiredAt = sql.NullInt64{Int64: expiredAt, Valid: true}
				expiringId[id.Base58()] = nil
			}
			err = bk.InsertUrl(&UrlEntry{Id: uint64(id), Url: url, ExpireAt: sqlExpiredAt})
			if err != nil {
				t.Fatal("failed on insert entry.", err)
			}
			idUrlMap[id.Base58()] = url
		}

		for j := 0; j < 20; j++ {
			nonexists = append(nonexists, node.Generate().Base58())
		}

		bk.Close()
		files = append(files, f.Name())
	}

	node, err := snowflake.NewNode(int64(dbCount))
	if err != nil {
		t.Fatal("failed on creating snowflake node.", err)
	}
	for j := 0; j < 20; j++ {
		nonexists = append(nonexists, node.Generate().Base58())
	}

	return files, idUrlMap, expiringId, expiredAt, nonexists
}

func TestRedirecter_Normal(t *testing.T) {
	files, idUrlMap, expiringId, expiredAt, nonexists := initTest(3, 100, t)
	for _, f := range files {
		defer os.Remove(f)
	}

	redirecter, err := NewRedirecter(files, "https://r.mrzm.io", false, true)
	if err != nil {
		t.Fatal("failed on creating redirecter.", err)
	}

	testCommon(t, idUrlMap, redirecter, expiredAt, expiringId, nonexists, "https://r.mrzm.io")
}

func TestRedirecter_NoCache(t *testing.T) {
	files, idUrlMap, expiringId, expiredAt, nonexists := initTest(3, 100, t)
	for _, f := range files {
		defer os.Remove(f)
	}

	redirecter, err := NewRedirecter(files, "https://r.mrzm.io/nocache", false, false)
	if err != nil {
		t.Fatal("failed on creating redirecter.", err)
	}

	testCommon(t, idUrlMap, redirecter, expiredAt, expiringId, nonexists, "https://mrzm.io/nocache")
}

func TestRedirecter_strict(t *testing.T) {
	files, idUrlMap, expiringId, expiredAt, nonexists := initTest(3, 100, t)
	for _, f := range files {
		defer os.Remove(f)
	}

	redirecter, err := NewRedirecter(files, "https://r.mrzm.io/strict", true, true)
	if err != nil {
		t.Fatal("failed on creating redirecter.", err)
	}

	testCommon(t, idUrlMap, redirecter, expiredAt, expiringId, nonexists, "https://r.mrzm.io/strict")

	for k, _ := range idUrlMap {
		check404("POST", "https://r.mrzm.io/strict/"+k, redirecter, t)
		check404("GET", "https://r.mrzm.io/"+k, redirecter, t)
		check404("GET", "https://mrzm.io/strict/"+k, redirecter, t)
	}
	check404("GET", "https://r.mrzm.io/strict/", redirecter, t)
	check404("GET", "https://r.mrzm.io/strict/9+", redirecter, t)
}

func TestRedirecter_fsnotify(t *testing.T) {
	files, idUrlMap, expiringId, expiredAt, nonexists := initTest(3, 100, t)
	for _, f := range files {
		defer os.Remove(f)
	}

	redirecter, err := NewRedirecter(files, "https://r.mrzm.io/fsnotify", false, true)
	if err != nil {
		t.Fatal("failed on creating redirecter.", err)
	}

	testCommon(t, idUrlMap, redirecter, expiredAt, expiringId, nonexists, "https://r.mrzm.io/fsnotify")

	idToDelete := ""
	for k, _ := range idUrlMap {
		if _, found := expiringId[k]; !found {
			idToDelete = k
			break
		}
	}
	deleteId(files, idToDelete, t)
	delete(idUrlMap, idToDelete)
	check404("GET", "https://r.mrzm.io/fsnotify/"+idToDelete, redirecter, t)
	for k, v := range idUrlMap {
		if _, ok := expiringId[k]; ok {
			check404("GET", "https://r.mrzm.io/fsnotify/"+k, redirecter, t)
		} else {
			check302("GET", "https://r.mrzm.io/fsnotify/"+k, v, redirecter, t)
		}
	}
}

func BenchmarkNewRedirecter(b *testing.B) {
	files, idUrlMap, _, _, nonexisted := initTest(3, 1000, b)
	for _, f := range files {
		defer os.Remove(f)
	}

	redirecter, err := NewRedirecter(files, "https://r.mrzm.io", false, true)
	if err != nil {
		b.Fatal("failed on creating redirecter.", err)
	}
	idStr := nonexisted
	for k, _ := range idUrlMap {
		idStr = append(idStr, k)
	}

	counter := atomic.Int32{}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reqUrl := "https://r.mrzm.io/" + idStr[int(counter.Add(1))%len(idStr)]
			req, err := http.NewRequest("GET", reqUrl, nil)
			if err != nil {
				b.Fatal("failed on creating http req.", err)
			}

			rr := httptest.NewRecorder()

			redirecter.ServeHTTP(rr, req)
		}
	})
}

func BenchmarkNewRedirecterNoCache(b *testing.B) {
	files, idUrlMap, _, _, nonexisted := initTest(3, 1000, b)
	for _, f := range files {
		defer os.Remove(f)
	}

	redirecter, err := NewRedirecter(files, "https://r.mrzm.io", false, false)
	if err != nil {
		b.Fatal("failed on creating redirecter.", err)
	}
	idStr := nonexisted
	for k, _ := range idUrlMap {
		idStr = append(idStr, k)
	}

	counter := atomic.Int32{}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reqUrl := "https://r.mrzm.io/" + idStr[int(counter.Add(1))%len(idStr)]
			req, err := http.NewRequest("GET", reqUrl, nil)
			if err != nil {
				b.Fatal("failed on creating http req.", err)
			}

			rr := httptest.NewRecorder()

			redirecter.ServeHTTP(rr, req)
		}
	})
}

func check404(method string, url string, redirecter *Redirecter, t *testing.T) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal("failed on creating http req.", err)
	}

	rr := httptest.NewRecorder()

	redirecter.ServeHTTP(rr, req)
	if rr.Result().StatusCode != 404 {
		t.Error("not 404")
	}
}

func check302(method string, reqUrl string, expectedLocation string, redirecter *Redirecter, t *testing.T) {
	req, err := http.NewRequest(method, reqUrl, nil)
	if err != nil {
		t.Fatal("failed on creating http req.", err)
	}

	rr := httptest.NewRecorder()

	redirecter.ServeHTTP(rr, req)
	if rr.Result().StatusCode != 302 {
		t.Error("not 302")
	}
	url, err := rr.Result().Location()
	if err != nil {
		t.Error("failed on get location.", err)
	}
	if url.String() != expectedLocation {
		t.Error("redirect location not match")
	}
}

func testCommon(t *testing.T, idUrlMap map[string]string, redirecter *Redirecter, expiredAt int64, expiringId map[string]interface{}, nonexists []string, baseUrl string) {
	baseUrlUrl, err := url.Parse(baseUrl)
	if err != nil {
		t.Fatal("failed to convert baseUrl.", err)
	}
	for k, v := range idUrlMap {
		check302("GET", baseUrlUrl.JoinPath(k).String(), v, redirecter, t)
	}

	for _, ne := range nonexists {
		check404("GET", baseUrlUrl.JoinPath(ne).String(), redirecter, t)
	}

	time.Sleep(time.Unix(expiredAt, 0).Sub(time.Now()))
	for k, v := range idUrlMap {
		if _, ok := expiringId[k]; ok {
			check404("GET", baseUrlUrl.JoinPath(k).String(), redirecter, t)
		} else {
			check302("GET", baseUrlUrl.JoinPath(k).String(), v, redirecter, t)
		}
	}

	for _, ne := range nonexists {
		check404("GET", baseUrlUrl.JoinPath(ne).String(), redirecter, t)
	}
}
