package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/EvolutionAPI/evo-bot-runtime/internal/config"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiService "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
	debounceService "github.com/EvolutionAPI/evo-bot-runtime/pkg/debounce/service"
	dispatchService "github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/guard"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/media"
	pipelineHandler "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/handler"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
	pipelineService "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/service"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/presence"
)

func main() {
	// Step 1: config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Step 2: Redis client + connectivity check
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("invalid REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis connection failed: %v", err)
	}

	// Step 3: redsync
	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)

	// Step 4: repository
	pipelineRepo := repository.NewPipelineRepository(rdb, rs)

	// Step 5: debounce engine
	debounce := debounceService.NewDebounceEngine(pipelineRepo)

	// Step 6: AI adapter (URL comes from each event's outgoing_url)
	// INOVE fork: the guard mutes conversations that look like a bot loop.
	botGuard := guard.New(guard.ConfigFromEnv(), guard.NewRedisStore(rdb), guard.NewCRMNotifierFromEnv())
	aiAdapter := &guardedAI{AIAdapter: aiService.NewAIAdapter(cfg.AICallTimeoutSeconds), guard: botGuard}

	// Step 7: dispatch engine (sends secret header on postback to CRM)
	// INOVE fork: "digitando..." + human pacing between message parts.
	typist := presence.NewFromEnv()
	var dispatch dispatchService.DispatchEngine
	if typist != nil {
		dispatch = dispatchService.NewDispatchEngineWithTypist(cfg.BotRuntimeSecret, typist)
	} else {
		dispatch = dispatchService.NewDispatchEngine(cfg.BotRuntimeSecret)
	}

	// Step 8: pipeline service
	pipeline := pipelineService.NewPipelineService(pipelineRepo, debounce, aiAdapter, dispatch)
	if err := pipeline.Start(); err != nil {
		log.Fatalf("pipeline service failed to start: %v", err)
	}

	// Step 9: handler + routes
	// INOVE fork: media messages without text are transcribed (audio) or replaced
	// by an instruction for the agent, so the lead always gets an answer.
	enriched := &enrichingPipeline{PipelineService: pipeline, enricher: media.NewFromEnv(), typist: typist, guard: botGuard}
	handler := pipelineHandler.NewHandler(pipelineRepo, enriched, cfg.BotRuntimeSecret)
	r := gin.New()
	r.Use(gin.Recovery())
	handler.RegisterRoutes(r)

	// Step 10: start server (non-blocking)
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: r}
	go func() {
		log.Printf("evo-bot-runtime starting on %s", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	}()

	// Step 11: wait for SIGTERM or SIGINT
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
	log.Printf("evo-bot-runtime shutting down")

	// Step 12: graceful shutdown — 10s budget for in-flight HTTP requests
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// Step 13: stop pipeline (cancel in-flight goroutines, drain poller)
	pipeline.Shutdown(shutdownCtx)
}

// enrichingPipeline fills empty message content before the normal pipeline runs.
type enrichingPipeline struct {
	pipelineService.PipelineService
	enricher *media.Enricher
	typist   *presence.Typist
	guard    *guard.Guard
}

func (p *enrichingPipeline) Process(ctx context.Context, event *model.MessageEvent) error {
	// Guard runs on the raw content, before transcription spends anything.
	if !p.guard.CheckIncoming(ctx, event.ConversationID, event.MessageContent, event.Metadata) {
		return p.PipelineService.Cancel(event.ContactID, event.ConversationID)
	}
	p.typist.Remember(event.ConversationID, event.Metadata)
	p.enricher.Enrich(ctx, event)
	return p.PipelineService.Process(ctx, event)
}

// guardedAI enforces the per-conversation turn caps before every AI call.
type guardedAI struct {
	aiService.AIAdapter
	guard *guard.Guard
}

func (a *guardedAI) Call(ctx context.Context, req *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
	if err := a.guard.AllowTurn(ctx, req.ConversationID, req.Metadata); err != nil {
		return nil, err
	}
	return a.AIAdapter.Call(ctx, req)
}
