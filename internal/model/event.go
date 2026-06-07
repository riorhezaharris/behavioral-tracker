package model

import (
	"encoding/json"
	"time"
)

type EventEnvelope struct {
	EventID    string          `json:"event_id"   binding:"required"`
	Type       string          `json:"type"       binding:"required"`
	SessionID  string          `json:"session_id" binding:"required"`
	Page       string          `json:"page"       binding:"required"`
	Timestamp  time.Time       `json:"timestamp"  binding:"required"`
	Properties json.RawMessage `json:"properties"`
}
