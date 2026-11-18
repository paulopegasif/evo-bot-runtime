// Package presence shows "digitando..." (WhatsApp composing presence) to the
// lead while the bot is "typing" each message part.
//
// INOVE fork. The Evolution Go connection (api_url + instance_token) is read
// from the CRM inbox using CRM_API_URL / CRM_API_TOKEN, so no extra secret is
// needed. Everything here is best effort: a failure never blocks a reply.
package presence

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type target struct {
	inboxID string
	number  string
}

type connection struct {
	apiURL  string
	token   string
	fetched time.Time
}

// Typist sends composing/paused presence for a conversation.
type Typist struct {
	crmURL   string
	crmToken string
	client   *http.Client
	targets  sync.Map // int64 conversation id -> target
	conns    sync.Map // inbox id -> connection
	ttl      time.Duration
}

// NewFromEnv returns nil when CRM_API_URL / CRM_API_TOKEN are not set.
func NewFromEnv() *Typist {
	u := strings.TrimRight(os.Getenv("CRM_API_URL"), "/")
	tok := os.Getenv("CRM_API_TOKEN")
	if u == "" || tok == "" || os.Getenv("TYPING_PRESENCE") == "off" {
		return nil
	}
	return New(u, tok)
}

func New(crmURL, crmToken string) *Typist {
	return &Typist{crmURL: crmURL, crmToken: crmToken, client: &http.Client{Timeout: 6 * time.Second}, ttl: 30 * time.Minute}
}

// Remember stores who to show the presence to, from the event metadata.
func (t *Typist) Remember(conversationID int64, meta map[string]any) {
	if t == nil || meta == nil {
		return
	}
	tg := target{}
	if v, ok := meta["inboxId"].(string); ok {
		tg.inboxID = v
	}
	if data, ok := meta["evoai_crm_data"].(map[string]any); ok && tg.inboxID == "" {
		if ib, ok := data["inbox"].(map[string]any); ok {
			tg.inboxID, _ = ib["id"].(string)
		}
	}
	if c, ok := meta["contact"].(map[string]any); ok {
		if p, _ := c["phone_number"].(string); p != "" {
			tg.number = strings.TrimPrefix(strings.TrimSpace(p), "+")
		} else if id, _ := c["identifier"].(string); strings.Contains(id, "@") {
			tg.number = id
		}
	}
	if tg.inboxID != "" && tg.number != "" {
		t.targets.Store(conversationID, tg)
	}
}

// Typing turns "digitando..." on (true) or off (false). Best effort.
func (t *Typist) Typing(ctx context.Context, conversationID int64, on bool) {
	if t == nil {
		return
	}
	v, ok := t.targets.Load(conversationID)
	if !ok {
		return
	}
	tg := v.(target)
	conn, err := t.connection(ctx, tg.inboxID)
	if err != nil {
		slog.Info("presence.skip", "conversation_id", conversationID, "error", err.Error())
		return
	}
	state := "paused"
	if on {
		state = "composing"
	}
	body, _ := json.Marshal(map[string]any{"number": tg.number, "state": state, "isAudio": false})
	rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(rctx, http.MethodPost, conn.apiURL+"/message/presence", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("apikey", conn.token)
	resp, err := t.client.Do(req)
	if err != nil {
		slog.Info("presence.failed", "conversation_id", conversationID, "error", err.Error())
		return
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		slog.Info("presence.failed", "conversation_id", conversationID, "status", resp.StatusCode)
		if resp.StatusCode == 401 || resp.StatusCode == 403 {
			t.conns.Delete(tg.inboxID)
		}
	}
}

func (t *Typist) connection(ctx context.Context, inboxID string) (connection, error) {
	if v, ok := t.conns.Load(inboxID); ok {
		c := v.(connection)
		if time.Since(c.fetched) < t.ttl {
			return c, nil
		}
	}
	rctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(rctx, http.MethodGet, t.crmURL+"/inboxes/"+inboxID, nil)
	req.Header.Set("api_access_token", t.crmToken)
	resp, err := t.client.Do(req)
	if err != nil {
		return connection{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return connection{}, fmt.Errorf("crm inbox status %d", resp.StatusCode)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return connection{}, err
	}
	if d, ok := raw["data"].(map[string]any); ok {
		raw = d
	}
	if d, ok := raw["payload"].(map[string]any); ok {
		raw = d
	}
	pc, _ := raw["provider_config"].(map[string]any)
	api, _ := pc["api_url"].(string)
	tok, _ := pc["instance_token"].(string)
	if api == "" || tok == "" {
		return connection{}, fmt.Errorf("inbox without evolution go connection")
	}
	c := connection{apiURL: strings.TrimRight(api, "/"), token: tok, fetched: time.Now()}
	t.conns.Store(inboxID, c)
	return c, nil
}
