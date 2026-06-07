package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/riorhezaharris/behavioral-tracker/internal/api"
	"github.com/riorhezaharris/behavioral-tracker/internal/metrics"
	"github.com/riorhezaharris/behavioral-tracker/internal/store"
	"github.com/riorhezaharris/behavioral-tracker/internal/stream"
	"github.com/riorhezaharris/behavioral-tracker/internal/worker"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	redisAddr := env("REDIS_ADDR", "localhost:6379")
	dbURL := env("DATABASE_URL", "postgres://tracker:tracker@localhost:5432/behavioral_tracker?sslmode=disable")
	workerCount := envInt("WORKER_COUNT", 4)
	port := env("PORT", "8080")

	rs := stream.New(redisAddr)
	if err := rs.Init(ctx); err != nil {
		log.Fatalf("redis stream init: %v", err)
	}
	defer rs.Close()

	pg, err := store.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("postgres init: %v", err)
	}
	defer pg.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	for i := 0; i < workerCount; i++ {
		w := worker.New(i, rs, pg, m)
		go w.Run(ctx)
	}
	log.Printf("started %d batch workers", workerCount)

	r := gin.New()
	r.Use(gin.Recovery())
	h := api.NewHandler(rs, pg, m)
	api.RegisterRoutes(r, h, reg)

	log.Printf("listening on :%s", port)
	if err := r.Run(fmt.Sprintf(":%s", port)); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
