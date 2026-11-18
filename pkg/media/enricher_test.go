package media

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

func meta(conv string) map[string]any {
	return map[string]any{"evoai_crm_data": map[string]any{"conversation": map[string]any{"id": conv}}}
}

func TestEnrich_KeepsText(t *testing.T) {
	e := &Enricher{}
	ev := &model.MessageEvent{MessageContent: "oi"}
	e.Enrich(context.Background(), ev)
	if ev.MessageContent != "oi" {
		t.Fatalf("got %q", ev.MessageContent)
	}
}

func TestEnrich_PlaceholderWhenNotConfigured(t *testing.T) {
	e := &Enricher{}
	ev := &model.MessageEvent{MessageContent: "  "}
	e.Enrich(context.Background(), ev)
	if ev.MessageContent != PlaceholderMedia {
		t.Fatalf("got %q", ev.MessageContent)
	}
}

func TestEnrich_TranscribesAudio(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/conversations/conv-1/messages"):
			if r.Header.Get("api_access_token") != "tok" {
				w.WriteHeader(401)
				return
			}
			io.WriteString(w, `{"data":[{"id":"m1","attachments":[{"file_type":"audio","data_url":"https://public.example/rails/blob/a.ogg"}]}]}`)
		case r.URL.Path == "/rails/blob/a.ogg":
			io.WriteString(w, "OGGDATA")
		case r.URL.Path == "/v1/audio/transcriptions":
			if err := r.ParseMultipartForm(1 << 20); err != nil || r.FormValue("model") == "" {
				w.WriteHeader(400)
				return
			}
			io.WriteString(w, `{"text":"quanto custa o curso?"}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	e := &Enricher{crmURL: srv.URL + "/api/v1", crmToken: "tok", openaiKey: "k", model: "m",
		openaiURL: srv.URL + "/v1/audio/transcriptions", client: srv.Client(), retryWait: time.Millisecond}
	ev := &model.MessageEvent{MessageID: "m1", Metadata: meta("conv-1")}
	e.Enrich(context.Background(), ev)
	if ev.MessageContent != transcriptPrefix+"quanto custa o curso?" {
		t.Fatalf("got %q", ev.MessageContent)
	}
}

func TestEnrich_FallsBackOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	e := &Enricher{crmURL: srv.URL, crmToken: "t", openaiKey: "k", client: srv.Client(), retryWait: time.Millisecond}
	ev := &model.MessageEvent{MessageID: "m1", Metadata: meta("c")}
	e.Enrich(context.Background(), ev)
	if ev.MessageContent != PlaceholderMedia {
		t.Fatalf("got %q", ev.MessageContent)
	}
}
