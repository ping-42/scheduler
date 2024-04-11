package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/ping-42/42lib/config"
	"github.com/ping-42/42lib/config/consts"
	"github.com/ping-42/42lib/db"
	"github.com/ping-42/42lib/db/models"
	"github.com/ping-42/42lib/logger"
	"gorm.io/gorm"
)

var schedulerLogger = logger.WithTestType("scheduler")
var gormClient *gorm.DB
var redisClient *redis.Client
var configuration config.Configuration

func init() {

	configuration = config.GetConfig()

	var err error
	gormClient, err = db.InitPostgreeDatabase(configuration.PostgreeDBDsn)
	if err != nil {
		schedulerLogger.Error("error while InitPostgreeDatabase()", err.Error())
		panic(err.Error())
	}

	redisClient, err = db.InitRedis(configuration.RedisHost, configuration.RedisPassword)
	if err != nil {
		schedulerLogger.Error("error while InitRedis()", err.Error())
		panic(err.Error())
	}
}

func main() {

	schedulerLogger.Info("Starting...")

	// select the supscriptions that have tasks for execution
	pengingSubscriptions, err := getPendingSubscriptions()

	if err != nil {
		logger.LogError(err.Error(), "getPendingSubscriptions err", schedulerLogger)
		return
	}

	schedulerLogger.Info(fmt.Sprintf("Found %v subscriptions/tasks pending for execution...", len(pengingSubscriptions)))
	if len(pengingSubscriptions) == 0 {
		schedulerLogger.Info("Nothing to do!")
		return
	}

	// select the same number of sensors that needs to do the tasks
	sensors, err := chooseSensorsByRank(len(pengingSubscriptions))
	if err != nil {
		logger.LogError(err.Error(), "chooseSensorsByRank err", schedulerLogger)
		return
	}

	// insert the new task to the db & publish to redis
	j := 0
	for i := 0; i < len(pengingSubscriptions); i++ {
		// if the subscriptions are more then the sensors, start from the first
		if i > len(sensors)-1 {
			j = 0
		}
		err := initSubscriptionТаск(context.Background(), pengingSubscriptions[i], sensors[j])
		if err != nil {
			logger.LogError(err.Error(), "initSubscriptionТаск err", schedulerLogger)
			continue
		}
		j++
	}
}

func getPendingSubscriptions() (clientSubscriptions []models.ClientSubscription, err error) {
	tx := gormClient.Debug().Where("tests_count_subscribed > tests_count_executed and ((last_execution_completed + period * interval '1 second') < ? or last_execution_completed IS NULL) AND is_active=true", time.Now()).Find(&clientSubscriptions)
	if tx.Error != nil {
		err = fmt.Errorf("getting ClientSubscription err:%v", tx.Error)
		return
	}
	return
}

func chooseSensorsByRank(numberOfSensors int) (sensors []models.Sensor, err error) {
	tx := gormClient.Raw(`
	SELECT sensors.*, sensor_ranks.current_rank
		FROM sensors
		JOIN sensor_ranks ON sensors.id = sensor_ranks.sensor_id
		ORDER BY sensor_ranks.current_rank DESC
	LIMIT ?`, numberOfSensors).Scan(&sensors)

	if tx.Error != nil {
		err = fmt.Errorf("getting Sensor err:%v", tx.Error)
		return
	}
	return
}

func initSubscriptionТаск(ctx context.Context, subscription models.ClientSubscription, sensor models.Sensor) (err error) {

	// layer between subscription.Opts and task.Opts
	taskOpts, err := factoryTaskOpts(subscription)
	if err != nil {
		return
	}

	// Insert the new task
	newTask := models.Task{
		ID:                   uuid.New(),
		TaskTypeID:           subscription.TaskTypeID,
		SensorID:             sensor.ID,
		ClientSubscriptionID: subscription.ID,
		TaskStatusID:         1, // INITIATED
		Opts:                 taskOpts,
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
