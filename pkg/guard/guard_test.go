package guard

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type memStore struct {
	mu sync.Mutex
	m  map[string]string
	n  map[string]int64
}

func newMem() *memStore { return &memStore{m: map[string]string{}, n: map[string]int64{}} }

func (s *memStore) Incr(_ context.Context, k string, _ time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n[k]++
	return s.n[k], nil
}
func (s *memStore) SetNX(_ context.Context, k, v string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[k]; ok {
		return false, nil
	}
	s.m[k] = v
	return true, nil
}
func (s *memStore) Get(_ context.Context, k string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}

type errStore struct{}

func (errStore) Incr(context.Context, string, time.Duration) (int64, error) {
	return 0, errors.New("down")
}
func (errStore) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return false, errors.New("down")
}
func (errStore) Get(context.Context, string) (string, bool, error) {
	return "", false, errors.New("down")
}

type fakeNotifier struct {
	mu    sync.Mutex
	texts []string
}

func (f *fakeNotifier) Notify(_ context.Context, _ map[string]any, t string) {
	f.mu.Lock()
	f.texts = append(f.texts, t)
	f.mu.Unlock()
}
func (f *fakeNotifier) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.texts)
}

func waitNotify(t *testing.T, f *fakeNotifier, want int) {
	t.Helper()
	for i := 0; i < 100 && f.count() < want; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if f.count() != want {
		t.Fatalf("notifications = %d, want %d", f.count(), want)
	}
}

func lead(num string) map[string]any {
	return map[string]any{"contact": map[string]any{"phone_number": "+" + num, "identifier": "1823@lid"}}
}

// Real messages from the 2026-11-22 incident (conversation #7).
var claro = []string{
	"Para resolver sua solicitação de forma rápida e prática, *acesse o Minha Claro* e utilize o autoatendimento:\n\n",
	"",
	"",
	"Não consegui entender. Vamos tentar uma última vez? Digite apenas o tema, por exemplo, \"fatura\".\n\n*Vamos tentar",
}

func TestClaroLoopIsStoppedBySpeedOfRepetition(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Blocklist = nil // prove the repetition trip alone catches it
	n := &fakeNotifier{}
	g := New(cfg, newMem(), n)
	ctx := context.Background()
	meta := lead("5511888887777")

	accepted := 0
	for round := 0; round < 50; round++ {
		for _, msg := range claro {
			if g.CheckIncoming(ctx, 7, msg, meta) {
				accepted++
			}
		}
	}
	// "Para resolver..." is blocked on its 3rd arrival: rounds 1 and 2 pass whole,
	// round 3 is dropped from the first message on.
	if accepted != 8 {
		t.Fatalf("accepted %d events, want 8", accepted)
	}
	if err := g.AllowTurn(ctx, 7, meta); !errors.Is(err, ErrMuted) {
		t.Fatalf("AllowTurn after trip = %v, want ErrMuted", err)
	}
	waitNotify(t, n, 1)
}

func TestShortRepeatedAnswersFromLeadsAreFine(t *testing.T) {
	g := New(DefaultConfig(), newMem(), nil)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		for _, m := range []string{"Sim", "sim!", "Ok 👍", "Quero sim", ""} {
			if !g.CheckIncoming(ctx, 1, m, lead("5561999998888")) {
				t.Fatalf("lead short answer %q blocked on round %d", m, i)
			}
		}
	}
}

func TestLongMessageTwiceIsFine(t *testing.T) {
	g := New(DefaultConfig(), newMem(), nil)
	ctx := context.Background()
	msg := "Oi, vi o anúncio do curso de auxiliar de saúde bucal e queria saber mais"
	for i := 0; i < 2; i++ {
		if !g.CheckIncoming(ctx, 1, msg, nil) {
			t.Fatal("a lead repeating a message twice must not be blocked")
		}
	}
}

func TestNormalizeIgnoresPunctuationAndCase(t *testing.T) {
	a := normalize("Para resolver sua solicitação, *acesse o Minha Claro*!!\n\n")
	b := normalize("para RESOLVER sua solicitação acesse o minha claro")
	if a != b {
		t.Fatalf("%q != %q", a, b)
	}
}

func TestBlocklistMutesImmediately(t *testing.T) {
	n := &fakeNotifier{}
	g := New(DefaultConfig(), newMem(), n)
	ctx := context.Background()
	if g.CheckIncoming(ctx, 9, "oi", lead("5511999910621")) {
		t.Fatal("Claro number must be blocked by default")
	}
	if g.CheckIncoming(ctx, 9, "oi de novo", lead("5511999910621")) {
		t.Fatal("muted conversation must stay muted")
	}
	waitNotify(t, n, 1) // notified once, not per message
}

func TestBlocklistFromEnv(t *testing.T) {
	t.Setenv("BOT_GUARD_BLOCKLIST", "+55 11 4004-0001, lixo")
	cfg := ConfigFromEnv()
	g := New(cfg, newMem(), nil)
	if g.CheckIncoming(context.Background(), 3, "oi", lead("551140040001")) {
		t.Fatal("number from BOT_GUARD_BLOCKLIST must be blocked")
	}
	if !g.CheckIncoming(context.Background(), 4, "oi", lead("5561999990000")) {
		t.Fatal("other numbers must pass")
	}
}

func TestContactNumberSources(t *testing.T) {
	cases := []struct {
		meta map[string]any
		want string
	}{
		{lead("5511999910621"), "5511999910621"},
		{map[string]any{"contact": map[string]any{"identifier": "5511999910621@s.whatsapp.net"}}, "5511999910621"},
		{map[string]any{"evoai_crm_data": map[string]any{"conversation": map[string]any{
			"additional_attributes": map[string]any{"evolution_go_chat_id": "5511999910621@s.whatsapp.net"}}}}, "5511999910621"},
		{nil, ""},
	}
	for i, c := range cases {
		if got := ContactNumber(c.meta); got != c.want {
			t.Errorf("case %d: got %q want %q", i, got, c.want)
		}
	}
}

func TestTurnCapPerHour(t *testing.T) {
	n := &fakeNotifier{}
	g := New(DefaultConfig(), newMem(), n)
	ctx := context.Background()
	for i := 1; i <= 30; i++ {
		if err := g.AllowTurn(ctx, 5, nil); err != nil {
			t.Fatalf("turn %d rejected: %v", i, err)
		}
	}
	if err := g.AllowTurn(ctx, 5, nil); !errors.Is(err, ErrMuted) {
		t.Fatalf("turn 31 = %v, want ErrMuted", err)
	}
	if g.CheckIncoming(ctx, 5, "olá", nil) {
		t.Fatal("incoming must be dropped once muted")
	}
	if err := g.AllowTurn(ctx, 6, nil); err != nil {
		t.Fatal("other conversations are not affected")
	}
	waitNotify(t, n, 1)
}

func TestTurnCapPerDay(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxTurnsHour = 1000
	g := New(cfg, newMem(), nil)
	ctx := context.Background()
	base := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 60; i++ {
		g.now = func() time.Time { return base.Add(time.Duration(i) * 20 * time.Minute) }
		if err := g.AllowTurn(ctx, 8, nil); err != nil {
			t.Fatalf("turn %d rejected: %v", i+1, err)
		}
	}
	if err := g.AllowTurn(ctx, 8, nil); !errors.Is(err, ErrMuted) {
		t.Fatal("turn 61 of the day must be rejected")
	}
}

func TestDisabledAndFailOpen(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	off := New(cfg, newMem(), nil)
	if !off.CheckIncoming(context.Background(), 1, "x", lead("5511999910621")) {
		t.Fatal("disabled guard must let everything through")
	}
	down := New(DefaultConfig(), errStore{}, nil)
	if !down.CheckIncoming(context.Background(), 1, claro[0], nil) || down.AllowTurn(context.Background(), 1, nil) != nil {
		t.Fatal("Redis errors must fail open")
	}
	var nilGuard *Guard
	if !nilGuard.CheckIncoming(context.Background(), 1, "x", nil) || nilGuard.AllowTurn(context.Background(), 1, nil) != nil {
		t.Fatal("nil guard must be a no-op")
	}
}
