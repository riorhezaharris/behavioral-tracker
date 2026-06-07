package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riorhezaharris/behavioral-tracker/internal/model"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, connStr string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

// BulkInsert inserts a batch of events using a multi-row INSERT.
// ON CONFLICT DO NOTHING ensures client-generated event_id deduplication.
func (s *PostgresStore) BulkInsert(ctx context.Context, events []model.EventEnvelope) error {
	if len(events) == 0 {
		return nil
	}

	args := make([]interface{}, 0, len(events)*6)
	placeholders := make([]string, 0, len(events))

	for i, e := range events {
		base := i * 6
		props := e.Properties
		if props == nil {
			props = json.RawMessage("{}")
		}
		args = append(args, e.EventID, e.Type, e.SessionID, e.Page, e.Timestamp, string(props))
		placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d::jsonb)", base+1, base+2, base+3, base+4, base+5, base+6))
	}

	query := fmt.Sprintf(
		`INSERT INTO events (event_id,type,session_id,page,timestamp,properties) VALUES %s ON CONFLICT (event_id) DO NOTHING`,
		strings.Join(placeholders, ","),
	)
	_, err := s.pool.Exec(ctx, query, args...)
	return err
}

// InsertOne writes a single event synchronously — used by the sync path.
func (s *PostgresStore) InsertOne(ctx context.Context, e model.EventEnvelope) error {
	props := e.Properties
	if props == nil {
		props = json.RawMessage("{}")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO events (event_id,type,session_id,page,timestamp,properties)
		VALUES ($1,$2,$3,$4,$5,$6::jsonb)
		ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.Type, e.SessionID, e.Page, e.Timestamp, string(props),
	)
	return err
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}
