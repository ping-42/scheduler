package scheduler

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

	schedulerLogger.Info("Sheduler triggered...")

	// select the supscriptions that have tasks for execution
	pengingSubscriptions, err := getPendingSubscriptions(*dbClient)

	if err != nil {
		schedulerLogger.Error("getPendingSubscriptions err", schedulerLogger)
		return
	}

	schedulerLogger.Info(fmt.Sprintf("Found %v subscriptions/tasks pending for execution...", len(pengingSubscriptions)))
	if len(pengingSubscriptions) == 0 {
		schedulerLogger.Info("Nothing to do!")
		return
	}

	//---------ORIGINAl---------
	// select the same number of sensors that needs to do the tasks
	sensors, err := chooseSensorsByRank(*dbClient, len(pengingSubscriptions))
	if err != nil {
		schedulerLogger.Error("chooseSensorsByRank err", schedulerLogger)
		return
	}
	if len(sensors) == 0 {
		schedulerLogger.Error("no available sensors!")
		return
	}

	// insert the new task to the db & publish to redis
	j := 0
	for i := 0; i < len(pengingSubscriptions); i++ {
		// if the subscriptions are more then the sensors, start from the first
		if i > len(sensors)-1 {
			j = 0
		}
		err := initSubscriptionTask(context.Background(), pengingSubscriptions[i], sensors[j].ID, *dbClient, redisClient, schedulerLogger)
		if err != nil {
			schedulerLogger.Error("initSubscriptionTask err", schedulerLogger, err)
			continue
		}
		j++
	}
	// ---------ORIGINAl---------

	// //----------TMP-HACK---------
	// activeSensors, err := redisClient.Keys(constants.RedisActiveSensorsKeyPrefix + "*").Result()
	// if err != nil {
	// 	schedulerLogger.Errorf("failed to retrieve keys from Redis: %v", err)
	// 	return
	// }
	// if len(activeSensors) == 0 {
	// 	schedulerLogger.Errorf("no active sensors")
	// 	return
	// }
	// // normalise values: [active_sensors<value>] -> [<value>]
	// // TODO: there should be a way to skip this
	// for k, v := range activeSensors {
	// 	activeSensors[k] = strings.TrimPrefix(v, constants.RedisActiveSensorsKeyPrefix)
	// 	if _, err = uuid.Parse(activeSensors[k]); err != nil {
	// 		schedulerLogger.Errorf("failed to parse active sensor UUID from redis: %v, ranking aborted", activeSensors[k])
	// 		return
	// 	}
	// }
	// schedulerLogger.Info("Currently active sensors:", activeSensors)
	// // insert the new task to the db & publish to redis
	// j := 0
	// for i := 0; i < len(pengingSubscriptions); i++ {
	// 	// if the subscriptions are more then the sensors, start from the first
	// 	if i > len(activeSensors)-1 {
	// 		j = 0
	// 	}

	// 	activeSensorId, err := uuid.Parse(activeSensors[j])
	// 	if err != nil {
	// 		schedulerLogger.Errorf("failed to parse active sensor UUID from redis: %v, ranking aborted", activeSensors[j])
	// 		return
	// 	}

	// 	err = initSubscriptionTask(context.Background(), pengingSubscriptions[i], activeSensorId, *dbClient, redisClient, schedulerLogger)
	// 	if err != nil {
	// 		schedulerLogger.Error("initSubscriptionTask err", schedulerLogger, err)
	// 		continue
	// 	}
	// 	j++
	// }
	// //----------TMP-HACK-END--------
}

func initSubscriptionTask(ctx context.Context, subscription models.Subscription, sensorId uuid.UUID, gormClient gorm.DB, redisClient *redis.Client, schedulerLogger *logrus.Entry) (err error) {

	// layer between subscription.Opts and task.Opts
	taskOpts, err := factoryTaskOpts(subscription)
	if err != nil {
		return
	}

	// Insert the new task
	newTask := models.Task{
		ID:             uuid.New(),
		TaskTypeID:     subscription.TaskTypeID,
		SensorID:       sensorId,
		SubscriptionID: subscription.ID,
		TaskStatusID:   1, // INITIATED
		Opts:           taskOpts,
		CreatedAt:      time.Now(),
	}
	tx := gormClient.Create(&newTask)
	if tx.Error != nil {
		err = fmt.Errorf("creating newTask err:%v", tx.Error)
		return
	}

	jsonMessage, err := factoryTaskMessage(newTask)
	if err != nil {
		return
	}

	schedulerLogger.Info(fmt.Sprintf("Publishing tasksID:%v, sensorID:%v", newTask.ID, newTask.SensorID))

	// Publish the message to the channel
	result := redisClient.Publish(consts.SchedulerNewTaskChannel, jsonMessage)
	if result.Err() != nil {
		err = fmt.Errorf("redisClient.Publish err:%v, tasksID:%v, sensorID:%v", result.Err().Error(), newTask.ID, newTask.SensorID)
		return
	}

	// Update the task status to PUBLISHED_TO_REDIS_BY_SCHEDULER
	updateTx := gormClient.Model(&models.Task{}).Where("id = ?", newTask.ID).Update("task_status_id", 2)
	if updateTx.Error != nil {
		err = fmt.Errorf("updating TaskStatusID err:%v", updateTx.Error)
		return
	}

	return
}

func getPendingSubscriptions(gormClient gorm.DB) (clientSubscriptions []models.Subscription, err error) {
	tx := gormClient.Where("tests_count_subscribed > tests_count_executed and ((last_execution_completed + period * interval '1 second') < ? or last_execution_completed IS NULL) AND is_active=true", time.Now()).Find(&clientSubscriptions)
	if tx.Error != nil {
		err = fmt.Errorf("getting ClientSubscription err:%v", tx.Error)
		return
	}
	return
}

func chooseSensorsByRank(gormClient gorm.DB, numberOfSensors int) (sensors []models.Sensor, err error) { //nolint

	// TODO: should be optimised; also check indexes;
	err = gormClient.Raw(`WITH cte_sensors_latest AS (
		SELECT max(id) as id
			, sensor_id
		FROM sensor_ranks
		WHERE created_at > NOW() - INTERVAL '60 minutes'
			AND rank > 0
		GROUP BY sensor_id
		LIMIT ?
	)
	SELECT sr.sensor_id as id
	FROM cte_sensors_latest cte
	INNER JOIN sensor_ranks sr ON (cte.id = sr.id)
	GROUP BY sr.sensor_id
	ORDER BY SUM(sr.rank + sr.distribution_rank) DESC`, numberOfSensors).Scan(&sensors).Error

	return
}
