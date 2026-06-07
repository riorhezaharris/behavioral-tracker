package stream

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/riorhezaharris/behavioral-tracker/internal/model"
)

const (
	streamKey     = "events"
	consumerGroup = "batch-workers"
)

type RedisStream struct {
	client *redis.Client
}

func New(addr string) *RedisStream {
	return &RedisStream{
		client: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

// Init creates the consumer group, tolerating "already exists" errors.
func (s *RedisStream) Init(ctx context.Context) error {
	err := s.client.XGroupCreateMkStream(ctx, streamKey, consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (s *RedisStream) Write(ctx context.Context, e model.EventEnvelope) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{"data": string(data)},
	}).Err()
}

// ReadGroup reads up to count messages, blocking for at most block duration.
func (s *RedisStream) ReadGroup(ctx context.Context, consumer string, count int64, block time.Duration) ([]model.EventEnvelope, []string, error) {
	msgs, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    consumerGroup,
		Consumer: consumer,
		Streams:  []string{streamKey, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err == redis.Nil || len(msgs) == 0 {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	var events []model.EventEnvelope
	var ids []string
	for _, msg := range msgs[0].Messages {
		raw, ok := msg.Values["data"].(string)
		if !ok {
			continue
		}
		var e model.EventEnvelope
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			continue
		}
		events = append(events, e)
		ids = append(ids, msg.ID)
	}
	return events, ids, nil
}

func (s *RedisStream) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return s.client.XAck(ctx, streamKey, consumerGroup, ids...).Err()
}

func (s *RedisStream) Close() error {
	return s.client.Close()
}
