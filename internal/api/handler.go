package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/riorhezaharris/behavioral-tracker/internal/metrics"
	"github.com/riorhezaharris/behavioral-tracker/internal/model"
	"github.com/riorhezaharris/behavioral-tracker/internal/store"
	"github.com/riorhezaharris/behavioral-tracker/internal/stream"
)

type Handler struct {
	stream  *stream.RedisStream
	store   *store.PostgresStore
	metrics *metrics.Metrics
}

func NewHandler(s *stream.RedisStream, p *store.PostgresStore, m *metrics.Metrics) *Handler {
	return &Handler{stream: s, store: p, metrics: m}
}

// HandleAsync writes the event to the Redis Event Stream and returns 202 immediately.
func (h *Handler) HandleAsync(c *gin.Context) {
	var e model.EventEnvelope
	if err := c.ShouldBindJSON(&e); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	start := time.Now()
	if err := h.stream.Write(c.Request.Context(), e); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue event"})
		return
	}

	h.metrics.AsyncEventsTotal.Inc()
	h.metrics.AsyncDuration.Observe(time.Since(start).Seconds())
	c.Status(http.StatusAccepted)
}

// HandleSync writes the event directly to PostgreSQL and returns 200 after the write.
func (h *Handler) HandleSync(c *gin.Context) {
	var e model.EventEnvelope
	if err := c.ShouldBindJSON(&e); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	start := time.Now()
	if err := h.store.InsertOne(c.Request.Context(), e); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to store event"})
		return
	}

	h.metrics.SyncEventsTotal.Inc()
	h.metrics.SyncDuration.Observe(time.Since(start).Seconds())
	c.Status(http.StatusOK)
}
