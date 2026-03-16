package storage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFilerHTTPStorageCRUD(t *testing.T) {
	objects := map[string][]byte{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			body := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(body)
			objects[strings.TrimPrefix(r.URL.Path, "/")] = body
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			if r.URL.RawQuery != "" {
				_ = json.NewEncoder(w).Encode(map[string]any{"Entries": []map[string]string{{"name": "a.frag"}}})
				return
			}
			value, ok := objects[strings.TrimPrefix(r.URL.Path, "/")]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("Range") != "" {
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(value[:4])
				return
			}
			_, _ = w.Write(value)
		case http.MethodDelete:
			delete(objects, strings.TrimPrefix(r.URL.Path, "/"))
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	store := NewFilerHTTPStorage(server.URL)
	ctx := context.Background()
	if err := store.Put(ctx, "recordings/cam01/1.frag", []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	data, err := store.Get(ctx, "recordings/cam01/1.frag")
	if err != nil || string(data) != "abcdef" {
		t.Fatalf("Get() = %q, %v", string(data), err)
	}
	partial, err := store.GetRange(ctx, "recordings/cam01/1.frag", 0, 4)
	if err != nil || string(partial) != "abcd" {
		t.Fatalf("GetRange() = %q, %v", string(partial), err)
	}
	items, err := store.List(ctx, "recordings/cam01")
	if err != nil || len(items) != 1 {
		t.Fatalf("List() = %v, %v", items, err)
	}
	if err := store.Delete(ctx, "recordings/cam01/1.frag"); err != nil {
		t.Fatal(err)
	}
}
