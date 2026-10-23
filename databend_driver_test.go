package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	godatabend "github.com/datafuselabs/databend-go"

	"github.com/databendcloud/bend-ingest-kafka/config"
)

// Exercise the real SQL driver: a COPY must keep the gateway's cluster hint
// across repeated polls of the same page, even while that cluster is busy.
func TestCopyIntoPreservesDriverRouteHint(t *testing.T) {
	const (
		queryID   = "copy-route-hint-test"
		routeHint = "clcl-00000-test"
		nodeID    = "copy-node-0"
		pageURI   = "/v1/query/" + queryID + "/page/0"
		finalURI  = "/v1/query/" + queryID + "/final"
	)
	type queryResponse struct {
		ID       string `json:"id"`
		NodeID   string `json:"node_id"`
		State    string `json:"state"`
		NextURI  string `json:"next_uri"`
		FinalURI string `json:"final_uri"`
	}
	var starts, polls, finals atomic.Int32
	requireRouting := func(w http.ResponseWriter, r *http.Request) bool {
		if got := r.Header.Get("X-DATABEND-ROUTE-HINT"); got != routeHint {
			t.Errorf("%s route hint = %q, want %q", r.URL.Path, got, routeHint)
			http.Error(w, "query not found on another cluster", http.StatusNotFound)
			return false
		}
		if got := r.Header.Get("X-DATABEND-STICKY-NODE"); got != nodeID {
			t.Errorf("%s sticky node = %q, want %q", r.URL.Path, got, nodeID)
			http.Error(w, "query not found on another node", http.StatusNotFound)
			return false
		}
		return true
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/session/login":
			_, _ = w.Write([]byte(`{"version":"test"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/session/logout":
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/query":
			starts.Add(1)
			var request struct {
				SQL string `json:"sql"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode COPY request: %v", err)
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if !strings.HasPrefix(request.SQL, "COPY INTO default.raw ") {
				t.Errorf("unexpected SQL: %s", request.SQL)
			}
			w.Header().Set("X-DATABEND-ROUTE-HINT", routeHint)
			_ = json.NewEncoder(w).Encode(queryResponse{
				ID: queryID, NodeID: nodeID, State: "Running",
				NextURI: pageURI, FinalURI: finalURI,
			})
		case r.Method == http.MethodGet && r.URL.Path == pageURI:
			if !requireRouting(w, r) {
				return
			}
			response := queryResponse{
				ID: queryID, NodeID: nodeID, State: "Running",
				NextURI: pageURI, FinalURI: finalURI,
			}
			if polls.Add(1) >= 2 {
				response.State = "Succeeded"
				response.NextURI = ""
			}
			// Poll responses deliberately omit the route-hint header. The driver
			// must retain the hint from the initial response, not generate a new one.
			_ = json.NewEncoder(w).Encode(response)
		case r.Method == http.MethodGet && r.URL.Path == finalURI:
			if !requireRouting(w, r) {
				return
			}
			finals.Add(1)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dsn := "databend://databend:databend@" + strings.TrimPrefix(server.URL, "http://") + "/default?sslmode=disable&query_result_format=json"
	db, err := sql.Open("databend", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ingester := &databendIngester{
		db: db,
		databendIngesterCfg: &config.Config{
			DatabendTable: "default.raw",
			CopyPurge:     true,
		},
	}
	if err := ingester.copyInto([]*godatabend.StageLocation{{Name: "otel_tmp", Path: "batch/test.ndjson.zst"}}); err != nil {
		t.Fatalf("COPY with repeated page polls failed: %v", err)
	}
	if starts.Load() != 1 || polls.Load() != 2 || finals.Load() != 1 {
		t.Fatalf("request counts: starts=%d polls=%d finals=%d, want 1/2/1", starts.Load(), polls.Load(), finals.Load())
	}
}
