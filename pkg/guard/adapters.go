package guard

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore implements Store on go-redis.
type RedisStore struct{ rdb *redis.Client }

func NewRedisStore(rdb *redis.Client) *RedisStore { return &RedisStore{rdb: rdb} }

func (s *RedisStore) Incr(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	n, err := s.rdb.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if n == 1 {
		s.rdb.Expire(ctx, key, ttl)
	}
	return n, nil
}

func (s *RedisStore) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return s.rdb.SetNX(ctx, key, value, ttl).Result()
}

func (s *RedisStore) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := s.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// CRMNotifier posts a private note in the CRM conversation.
type CRMNotifier struct {
	crmURL, token string
	client        *http.Client
}

// NewCRMNotifierFromEnv returns nil when CRM_API_URL / CRM_API_TOKEN are not set.
func NewCRMNotifierFromEnv() *CRMNotifier {
	u := strings.TrimRight(os.Getenv("CRM_API_URL"), "/")
	t := os.Getenv("CRM_API_TOKEN")
	if u == "" || t == "" {
		return nil
	}
	return &CRMNotifier{crmURL: u, token: t, client: &http.Client{Timeout: 10 * time.Second}}
}

func (n *CRMNotifier) Notify(ctx context.Context, meta map[string]any, text string) {
	if n == nil {
		return
	}
	conv := ConversationUUID(meta)
	if conv == "" {
		return
	}
	body, _ := json.Marshal(map[string]any{"content": text, "private": true, "message_type": "outgoing"})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, n.crmURL+"/conversations/"+url.PathEscape(conv)+"/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api_access_token", n.token)
	resp, err := n.client.Do(req)
	if err != nil {
		slog.Warn("guard.notify_failed", "conversation", conv, "error", err.Error())
		return
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		slog.Warn("guard.notify_failed", "conversation", conv, "status", resp.StatusCode)
	}
}
