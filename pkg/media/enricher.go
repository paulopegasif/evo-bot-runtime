// Package media turns incoming messages without text (audio, image, sticker…)
// into something the AI agent can act on, so the lead never goes unanswered.
//
// INOVE fork. When CRM_API_URL, CRM_API_TOKEN and OPENAI_API_KEY are set, audio
// attachments are downloaded from the CRM and transcribed with OpenAI. Otherwise
// (or on any failure) the empty content is replaced by a short instruction that
// tells the agent a media message arrived and it should ask the lead to type.
package media

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

const (
	PlaceholderMedia = "[O lead enviou um áudio, imagem ou figurinha sem texto, que não dá para abrir aqui. " +
		"Responda com gentileza, sem dizer que é robô, pedindo para a pessoa escrever a dúvida por mensagem.]"
	PlaceholderImage = "[O lead enviou uma imagem sem texto. Se fizer sentido no contexto, agradeça e pergunte com o que pode ajudar.]"
	transcriptPrefix = "[Áudio do lead, transcrito]: "
	maxAudioBytes    = 20 << 20
)

type Enricher struct {
	crmURL    string // e.g. http://evo-crm:3000/api/v1
	crmToken  string
	openaiKey string
	model     string
	openaiURL string
	client    *http.Client
	retryWait time.Duration
}

// NewFromEnv builds an Enricher from CRM_API_URL, CRM_API_TOKEN, OPENAI_API_KEY
// and TRANSCRIBE_MODEL (default gpt-4o-mini-transcribe).
func NewFromEnv() *Enricher {
	m := os.Getenv("TRANSCRIBE_MODEL")
	if m == "" {
		m = "gpt-4o-mini-transcribe"
	}
	return &Enricher{
		crmURL:    strings.TrimRight(os.Getenv("CRM_API_URL"), "/"),
		crmToken:  os.Getenv("CRM_API_TOKEN"),
		openaiKey: os.Getenv("OPENAI_API_KEY"),
		model:     m,
		openaiURL: "https://api.openai.com/v1/audio/transcriptions",
		client:    &http.Client{Timeout: 45 * time.Second},
		retryWait: 1500 * time.Millisecond,
	}
}

func (e *Enricher) canTranscribe() bool {
	return e.crmURL != "" && e.crmToken != "" && e.openaiKey != ""
}

// Enrich fills event.MessageContent when it is empty. It never returns an error:
// worst case the content becomes PlaceholderMedia.
func (e *Enricher) Enrich(ctx context.Context, event *model.MessageEvent) {
	if strings.TrimSpace(event.MessageContent) != "" {
		return
	}
	event.MessageContent = PlaceholderMedia
	if !e.canTranscribe() {
		slog.Info("media.placeholder", "conversation_id", event.ConversationID, "reason", "transcription_not_configured")
		return
	}
	convUUID := conversationUUID(event.Metadata)
	if convUUID == "" || event.MessageID == "" {
		return
	}
	att, err := e.findAttachment(ctx, convUUID, event.MessageID)
	if err != nil || att == nil {
		slog.Warn("media.attachment_not_found", "conversation_id", event.ConversationID, "error", err)
		return
	}
	switch att.FileType {
	case "audio":
		text, err := e.transcribe(ctx, att.DataURL)
		if err != nil || strings.TrimSpace(text) == "" {
			slog.Warn("media.transcription_failed", "conversation_id", event.ConversationID, "error", err)
			return
		}
		event.MessageContent = transcriptPrefix + strings.TrimSpace(text)
		slog.Info("media.transcribed", "conversation_id", event.ConversationID, "chars", len(text))
	case "image":
		event.MessageContent = PlaceholderImage
	}
}

func conversationUUID(meta map[string]any) string {
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

type attachment struct {
	FileType string `json:"file_type"`
	DataURL  string `json:"data_url"`
}

type crmMessage struct {
	ID          string       `json:"id"`
	Attachments []attachment `json:"attachments"`
}

// findAttachment looks the message up in the CRM. Attachments are stored right
// after the message, so retry a couple of times before giving up.
func (e *Enricher) findAttachment(ctx context.Context, convUUID, messageID string) (*attachment, error) {
	var lastErr error
	for i := 0; i < 3; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(e.retryWait):
			}
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, e.crmURL+"/conversations/"+url.PathEscape(convUUID)+"/messages", nil)
		req.Header.Set("api_access_token", e.crmToken)
		resp, err := e.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		var body struct {
			Data []crmMessage `json:"data"`
		}
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("crm messages: status %d: %v", resp.StatusCode, err)
			continue
		}
		for _, m := range body.Data {
			if m.ID == messageID && len(m.Attachments) > 0 && m.Attachments[0].DataURL != "" {
				a := m.Attachments[0]
				return &a, nil
			}
		}
		lastErr = fmt.Errorf("message %s has no attachment yet", messageID)
	}
	return nil, lastErr
}

// internalURL rewrites the public data_url host to the CRM host the runtime can
// reach directly (same path, signed ActiveStorage URL).
func (e *Enricher) internalURL(dataURL string) string {
	u, err := url.Parse(dataURL)
	base, err2 := url.Parse(e.crmURL)
	if err != nil || err2 != nil || base.Host == "" {
		return dataURL
	}
	u.Scheme, u.Host = base.Scheme, base.Host
	return u.String()
}

func (e *Enricher) transcribe(ctx context.Context, dataURL string) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, e.internalURL(dataURL), nil)
	resp, err := e.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("download: %w", err)
	}
	audio, err := io.ReadAll(io.LimitReader(resp.Body, maxAudioBytes))
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || len(audio) == 0 {
		return "", fmt.Errorf("download: status %d: %v", resp.StatusCode, err)
	}

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	_ = w.WriteField("model", e.model)
	_ = w.WriteField("language", "pt")
	fw, _ := w.CreateFormFile("file", "audio.ogg")
	_, _ = fw.Write(audio)
	_ = w.Close()

	oreq, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.openaiURL, &buf)
	oreq.Header.Set("Authorization", "Bearer "+e.openaiKey)
	oreq.Header.Set("Content-Type", w.FormDataContentType())
	oresp, err := e.client.Do(oreq)
	if err != nil {
		return "", fmt.Errorf("openai: %w", err)
	}
	defer oresp.Body.Close()
	var out struct {
		Text  string `json:"text"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(oresp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("openai decode: %w", err)
	}
	if oresp.StatusCode != http.StatusOK {
		msg := ""
		if out.Error != nil {
			msg = out.Error.Message
		}
		return "", fmt.Errorf("openai: status %d: %s", oresp.StatusCode, msg)
	}
	return out.Text, nil
}
