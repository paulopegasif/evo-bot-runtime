// Package guard stops the bot from talking forever to another bot (or to a
// spammer), which burns LLM tokens without helping any lead.
//
// INOVE fork. Incident 2026-11-22: the agent answered the Claro operator
// chatbot for 1h40 (196 AI turns, each one carrying the whole growing history).
//
// Three independent trips, all per conversation:
//   - blocklist: the contact number matches BOT_GUARD_BLOCKLIST;
//   - repetition: the same incoming text (min length) arrives N times in a window
//     (automated menus repeat themselves; people repeat short words like "Sim");
//   - turn cap: too many AI calls in one hour or one day.
//
// A trip mutes the conversation for BOT_GUARD_MUTE_HOURS: incoming events are
// dropped before any AI call, and a private note is posted once in the CRM so a
// human can take over. Redis failures fail open (the bot keeps answering).
package guard

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// ErrMuted is returned to the AI stage when the conversation must not be answered.
var ErrMuted = errors.New("guard: conversation muted")

// Store is the small slice of Redis the guard needs.
type Store interface {
	// Incr increments key and sets ttl when the key is new. Returns the new value.
	Incr(ctx context.Context, key string, ttl time.Duration) (int64, error)
	// SetNX sets key only when absent. Returns true when it was set.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// Get returns the value and whether the key exists.
	Get(ctx context.Context, key string) (string, bool, error)
}

// Notifier tells humans that the bot paused a conversation. Best effort.
type Notifier interface {
	Notify(ctx context.Context, meta map[string]any, text string)
}

type Config struct {
	Enabled      bool
	MaxTurnsHour int64
	MaxTurnsDay  int64
	RepeatMax    int64
	RepeatWindow time.Duration
	RepeatMinLen int
	MuteFor      time.Duration
	Blocklist    []string // digits only
}

// DefaultBlocklist holds numbers known to be automated service bots.
var DefaultBlocklist = []string{
	"5511999910621", // Claro atendimento automático (loop 2026-11-22)
}

func DefaultConfig() Config {
	return Config{
		Enabled:      true,
		MaxTurnsHour: 30, // real leads peaked at 21/h
		MaxTurnsDay:  60, // real leads peaked at 21/day
		RepeatMax:    3,
		RepeatWindow: 30 * time.Minute,
		RepeatMinLen: 20,
		MuteFor:      24 * time.Hour,
		Blocklist:    append([]string(nil), DefaultBlocklist...),
	}
}

// ConfigFromEnv reads BOT_GUARD_* variables on top of DefaultConfig.
func ConfigFromEnv() Config {
	c := DefaultConfig()
	if strings.EqualFold(os.Getenv("BOT_GUARD"), "off") {
		c.Enabled = false
	}
	c.MaxTurnsHour = envInt("BOT_GUARD_MAX_TURNS_HOUR", c.MaxTurnsHour)
	c.MaxTurnsDay = envInt("BOT_GUARD_MAX_TURNS_DAY", c.MaxTurnsDay)
	c.RepeatMax = envInt("BOT_GUARD_REPEAT_MAX", c.RepeatMax)
	c.RepeatWindow = time.Duration(envInt("BOT_GUARD_REPEAT_WINDOW_MIN", int64(c.RepeatWindow/time.Minute))) * time.Minute
	c.RepeatMinLen = int(envInt("BOT_GUARD_REPEAT_MIN_LEN", int64(c.RepeatMinLen)))
	c.MuteFor = time.Duration(envInt("BOT_GUARD_MUTE_HOURS", int64(c.MuteFor/time.Hour))) * time.Hour
	for _, n := range strings.Split(os.Getenv("BOT_GUARD_BLOCKLIST"), ",") {
		if d := digits(n); len(d) >= 8 {
			c.Blocklist = append(c.Blocklist, d)
		}
	}
	return c
}

func envInt(key string, def int64) int64 {
	if v, err := strconv.ParseInt(strings.TrimSpace(os.Getenv(key)), 10, 64); err == nil && v > 0 {
		return v
	}
	return def
}

type Guard struct {
	cfg      Config
	store    Store
	notifier Notifier
	now      func() time.Time
}

func New(cfg Config, store Store, notifier Notifier) *Guard {
	return &Guard{cfg: cfg, store: store, notifier: notifier, now: time.Now}
}

func muteKey(conv int64) string { return fmt.Sprintf("guard:mute:%d", conv) }

// CheckIncoming runs on every incoming event, before the media enricher and the
// pipeline. It returns false when the event must be dropped.
func (g *Guard) CheckIncoming(ctx context.Context, conv int64, content string, meta map[string]any) bool {
	if g == nil || !g.cfg.Enabled {
		return true
	}
	if g.muted(ctx, conv) {
		slog.Info("guard.drop", "conversation_id", conv, "reason", "muted")
		return false
	}
	if num := ContactNumber(meta); num != "" && g.blocked(num) {
		g.trip(ctx, conv, meta, "número na lista de bloqueio ("+num+")")
		return false
	}
	norm := normalize(content)
	if len([]rune(norm)) >= g.cfg.RepeatMinLen {
		sum := sha1.Sum([]byte(norm))
		key := fmt.Sprintf("guard:rep:%d:%s", conv, hex.EncodeToString(sum[:8]))
		n, err := g.store.Incr(ctx, key, g.cfg.RepeatWindow)
		if err == nil && n >= g.cfg.RepeatMax {
			g.trip(ctx, conv, meta, fmt.Sprintf("a mesma mensagem chegou %d vezes em %s (provável robô do outro lado)",
				n, g.cfg.RepeatWindow))
			return false
		}
	}
	return true
}

// AllowTurn runs right before each AI call and enforces the turn caps.
func (g *Guard) AllowTurn(ctx context.Context, conv int64, meta map[string]any) error {
	if g == nil || !g.cfg.Enabled {
		return nil
	}
	if g.muted(ctx, conv) {
		return ErrMuted
	}
	now := g.now()
	hourKey := fmt.Sprintf("guard:turns:h:%d:%s", conv, now.UTC().Format("2006010215"))
	dayKey := fmt.Sprintf("guard:turns:d:%d:%s", conv, now.UTC().Format("20060102"))
	if n, err := g.store.Incr(ctx, hourKey, 2*time.Hour); err == nil && n > g.cfg.MaxTurnsHour {
		g.trip(ctx, conv, meta, fmt.Sprintf("mais de %d respostas do bot em 1 hora", g.cfg.MaxTurnsHour))
		return ErrMuted
	}
	if n, err := g.store.Incr(ctx, dayKey, 26*time.Hour); err == nil && n > g.cfg.MaxTurnsDay {
		g.trip(ctx, conv, meta, fmt.Sprintf("mais de %d respostas do bot no dia", g.cfg.MaxTurnsDay))
		return ErrMuted
	}
	return nil
}

func (g *Guard) muted(ctx context.Context, conv int64) bool {
	_, ok, err := g.store.Get(ctx, muteKey(conv))
	return err == nil && ok
}

func (g *Guard) blocked(num string) bool {
	for _, b := range g.cfg.Blocklist {
		if b != "" && (num == b || strings.HasSuffix(num, b) || strings.HasSuffix(b, num)) {
			return true
		}
	}
	return false
}

// trip mutes the conversation; the first trip also notifies the CRM.
func (g *Guard) trip(ctx context.Context, conv int64, meta map[string]any, reason string) {
	set, err := g.store.SetNX(ctx, muteKey(conv), reason, g.cfg.MuteFor)
	if err != nil {
		slog.Error("guard.mute_failed", "conversation_id", conv, "error", err)
		return
	}
	if !set {
		return
	}
	slog.Warn("guard.tripped", "conversation_id", conv, "reason", reason, "mute", g.cfg.MuteFor.String())
	if g.notifier != nil {
		text := fmt.Sprintf("🛑 Bot pausado nesta conversa por %s: %s. Atendimento deve seguir com um humano.",
			humanDuration(g.cfg.MuteFor), reason)
		go g.notifier.Notify(context.Background(), meta, text)
	}
}

func humanDuration(d time.Duration) string {
	if d >= time.Hour && d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return d.String()
}

// normalize lowercases, drops punctuation/emoji and collapses spaces so that
// automated messages with tiny variations still match.
func normalize(s string) string {
	var b strings.Builder
	space := false
	for _, r := range strings.ToLower(s) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(r)
			space = false
		case !space && b.Len() > 0:
			b.WriteByte(' ')
			space = true
		}
	}
	return strings.TrimSpace(b.String())
}

func digits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ContactNumber extracts the contact phone (digits only) from the CRM metadata.
func ContactNumber(meta map[string]any) string {
	if meta == nil {
		return ""
	}
	if c, ok := meta["contact"].(map[string]any); ok {
		if p, _ := c["phone_number"].(string); digits(p) != "" {
			return digits(p)
		}
		if id, _ := c["identifier"].(string); id != "" {
			return digits(strings.SplitN(id, "@", 2)[0])
		}
	}
	if data, ok := meta["evoai_crm_data"].(map[string]any); ok {
		if c, ok := data["contact"].(map[string]any); ok {
			if p, _ := c["phone_number"].(string); digits(p) != "" {
				return digits(p)
			}
		}
		if conv, ok := data["conversation"].(map[string]any); ok {
			if aa, ok := conv["additional_attributes"].(map[string]any); ok {
				if id, _ := aa["evolution_go_chat_id"].(string); id != "" {
					return digits(strings.SplitN(id, "@", 2)[0])
				}
			}
		}
	}
	return ""
}

// ConversationUUID returns the CRM conversation UUID from the event metadata.
func ConversationUUID(meta map[string]any) string {
	data, _ := meta["evoai_crm_data"].(map[string]any)
	if data == nil {
		return ""
	}
	if conv, ok := data["conversation"].(map[string]any); ok {
		if id, ok := conv["id"].(string); ok {
			return id
		}
	}
	if id, ok := data["conversation_id"].(string); ok {
		return id
	}
	return ""
}
