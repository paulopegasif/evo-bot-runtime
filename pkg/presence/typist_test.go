package presence

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestTypingSendsComposingWithInboxConnection(t *testing.T) {
	var mu sync.Mutex
	var got []map[string]any
	evo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/message/presence" || r.Header.Get("apikey") != "inst-tok" {
			t.Errorf("unexpected request %s apikey=%q", r.URL.Path, r.Header.Get("apikey"))
		}
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		got = append(got, b)
		mu.Unlock()
	}))
	defer evo.Close()
	crmCalls := 0
	crm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		crmCalls++
		if r.URL.Path != "/inboxes/ib-1" || r.Header.Get("api_access_token") != "crm-tok" {
			t.Errorf("unexpected crm request %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"provider_config": map[string]any{"api_url": evo.URL + "/", "instance_token": "inst-tok"}}})
	}))
	defer crm.Close()

	ty := New(crm.URL, "crm-tok")
	ty.Remember(55, map[string]any{"inboxId": "ib-1", "contact": map[string]any{"phone_number": "+5562999990000"}})
	ty.Typing(context.Background(), 55, true)
	ty.Typing(context.Background(), 55, false)
	ty.Typing(context.Background(), 99, true) // unknown conversation: no call

	if crmCalls != 1 {
		t.Errorf("inbox connection should be cached, crm calls=%d", crmCalls)
	}
	if len(got) != 2 || got[0]["state"] != "composing" || got[1]["state"] != "paused" || got[0]["number"] != "5562999990000" {
		t.Errorf("unexpected presence calls: %v", got)
	}
}

func TestNilTypistIsSafe(t *testing.T) {
	var ty *Typist
	ty.Remember(1, map[string]any{})
	ty.Typing(context.Background(), 1, true)
}
