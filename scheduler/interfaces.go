// package scheduler

// import (
// 	"context"
// 	"time"

// 	"github.com/go-redis/redis"
// 	"github.com/sirupsen/logrus"
// 	"gorm.io/gorm"

// 	"github.com/ping-42/42lib/db/models"
// )

// // MessageBroker abstracts message distribution - can be replaced later
// type MessageBroker interface {
// 	// Publish sends a task to available sensors
// 	Publish(ctx context.Context, channel string, message []byte) (int64, error)
// }

// // RedisBroker implements MessageBroker using Redis pub/sub
// type RedisBroker struct {
// 	client *redis.Client
// }

// func NewRedisBroker(client *redis.Client) MessageBroker {
// 	return &RedisBroker{client: client}
// }

// func (r *RedisBroker) Publish(ctx context.Context, channel string, message []byte) (int64, error) {
// 	return r.client.Publish(channel, message).Result()
// }

// // TaskScheduler orchestrates task distribution
// type TaskScheduler interface {
// 	Schedule(ctx context.Context) error
// 	AssignTask(ctx context.Context, task *models.Task, sensor *models.Sensor) error
// }

// // ContextConfig holds timeout configurations
// type ContextConfig struct {
// 	DBTimeout    time.Duration
// 	RedisTimeout time.Duration
// }

// // DefaultScheduler implements TaskScheduler
// type DefaultScheduler struct {
// 	db     *gorm.DB
// 	broker MessageBroker
// 	logger *logrus.Entry
// 	config ContextConfig
// }

// func NewScheduler(db *gorm.DB, broker MessageBroker, logger *logrus.Entry, cfg ContextConfig) TaskScheduler {
// 	return &DefaultScheduler{
// 		db:     db,
// 		broker: broker,
// 		logger: logger,
// 		config: cfg,
// 	}
// }
