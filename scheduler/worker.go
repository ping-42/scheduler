package scheduler

/*
	The scheduler service is responsible for choosing sensors for any given task.
	Chooses sensors ordered by their <rank - {task count for the last 10 minutes}>
	in a round robin manner.
*/

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/ping-42/42lib/config/consts"
	"github.com/ping-42/42lib/db/models"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// TODO: the config & default config should be extracted in the 42lib
// ContextConfig holds timeout configurations
type ContextConfig struct {
	DBTimeout    time.Duration
	RedisTimeout time.Duration
}

// NewDefaultConfig returns standard timeout values
func NewDefaultConfig() ContextConfig {
	return ContextConfig{
		DBTimeout:    30 * time.Second,
		RedisTimeout: 5 * time.Second,
	}
}

func Work(minuteInterval time.Duration, redisClient *redis.Client, dbClient *gorm.DB, logger *logrus.Entry) {
	work(redisClient, dbClient, logger)
	ticker := time.NewTicker(minuteInterval * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		work(redisClient, dbClient, logger)
	}
}

// work is the entry point for the scheduler
func work(redisClient *redis.Client, dbClient *gorm.DB, schedulerLogger *logrus.Entry) {
	cfg := NewDefaultConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dbCtx, dbCancel := context.WithTimeout(ctx, cfg.DBTimeout)
	defer dbCancel()

	schedulerLogger.Info("Scheduler triggered...")

	pengingSubscriptions, err := getPendingSubscriptions(dbCtx, *dbClient)
	if err != nil {
		schedulerLogger.Error("getPendingSubscriptions err", err)
		return
	}

	sensors, err := chooseSensorsByRank(dbCtx, *dbClient)
	if err != nil {
		schedulerLogger.Error("chooseSensorsByRank err", err)
		return
	}

	// important to keep this check as panic *may* occur later
	if len(sensors) == 0 {
		schedulerLogger.Error("no available sensors")
		return
	}

	redisCtx, redisCancel := context.WithTimeout(ctx, cfg.RedisTimeout)
	defer redisCancel()

	for i := 0; i < len(pengingSubscriptions); i++ {
		// this ensures a round-robin when tasks are more than the sensors
		sensorIdx := i % len(sensors)

		err := initSubscriptionTask(redisCtx, pengingSubscriptions[i], sensors[sensorIdx].ID, *dbClient, redisClient, schedulerLogger)
		if err != nil {
			schedulerLogger.Errorf("initSubscriptionTask err: %v", err)
			continue
		}
	}
}

func initSubscriptionTask(ctx context.Context,
	subscription models.Subscription,
	sensorId uuid.UUID,
	gormClient gorm.DB,
	redisClient *redis.Client,
	schedulerLogger *logrus.Entry) (err error) {

	// layer between subscription.Opts and task.Opts
	taskOpts, err := factoryTaskOpts(subscription)
	if err != nil {
		return
	}

	newTask := models.Task{
		ID:             uuid.New(),
		TaskTypeID:     subscription.TaskTypeID,
		SensorID:       sensorId,
		SubscriptionID: subscription.ID,
		TaskStatusID:   models.TASK_STATUS_INITIATED_BY_SCHEDULER,
		Opts:           taskOpts,
		CreatedAt:      time.Now(),
	}
	tx := gormClient.Create(&newTask)
	if tx.Error != nil {
		err = fmt.Errorf("creating newTask err:%v", tx.Error)
		return
	}

	taskJson, err := factoryTaskMessage(newTask)
	if err != nil {
		return
	}

	schedulerLogger.Info(fmt.Sprintf("Publishing tasksID:%v, sensorID:%v", newTask.ID, newTask.SensorID))

	result := redisClient.WithContext(ctx).Publish(consts.SchedulerNewTaskChannel, taskJson)
	if result.Err() != nil {
		err = fmt.Errorf("redisClient.Publish err:%v, tasksID:%v, sensorID:%v", result.Err().Error(), newTask.ID, newTask.SensorID)
		return
	}

	receiverCount, err := result.Result()
	if err != nil {
		return fmt.Errorf("failed to get publish result: %w", err)
	}
	if receiverCount == 0 {
		return fmt.Errorf("no subscribers received task: taskID:%v, sensorID:%v", newTask.ID, newTask.SensorID)
	}

	// TODO: this should be done in batches to reduce DB load
	return tx.Model(&models.Task{}).Where("id = ?", newTask.ID).Update("task_status_id", 2).Error
}

func getPendingSubscriptions(ctx context.Context, gormClient gorm.DB) ([]models.Subscription, error) {
	var clientSubscriptions []models.Subscription
	tx := gormClient.WithContext(ctx).Where(
		"tests_count_subscribed > tests_count_executed and ((last_execution_completed + period * interval '1 second') < ? or last_execution_completed IS NULL) AND is_active=true",
		time.Now(),
	).Find(&clientSubscriptions)

	if tx.Error != nil {
		return nil, fmt.Errorf("getting ClientSubscription err:%w", tx.Error)
	}
	return clientSubscriptions, nil
}

func chooseSensorsByRank(ctx context.Context, gormClient gorm.DB) (sensors []models.Sensor, err error) { //nolint

	// TODO: check indexes; add a coefficient to reduce the task count subtraction;
	err = gormClient.Raw("" +
		`WITH cte_sensors_latest AS (
	SELECT max(id) AS id
		, sensor_id
	FROM sensor_ranks
	WHERE created_at > NOW() - INTERVAL '60 minutes'
		AND rank > 0
	GROUP BY sensor_id
)
SELECT sr.sensor_id AS id, st.task_count
FROM cte_sensors_latest cte
INNER JOIN sensor_ranks sr ON (cte.id = sr.id)
LEFT JOIN (
    SELECT sensor_id, COUNT(*) as task_count
    FROM tasks
    WHERE created_at > NOW() - INTERVAL '10 minutes'
    GROUP BY sensor_id
) AS st ON st.sensor_id = sr.sensor_id
ORDER BY (sr.rank - 
    CASE 
        WHEN st.task_count IS NOT NULL THEN st.task_count
        ELSE 0
    END
) DESC`).WithContext(ctx).Scan(&sensors).Error

	return
}
