package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/ping-42/42lib/config"
	"github.com/ping-42/42lib/config/consts"
	"github.com/ping-42/42lib/constants"
	"github.com/ping-42/42lib/db"
	"github.com/ping-42/42lib/db/models"
	"github.com/ping-42/42lib/logger"
	"github.com/robfig/cron/v3"
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

	c := cron.New()
	c.AddFunc("@every 5m", func() {
		// Call your function here
		fmt.Println("Function called every 5 minutes using cron")
	})
	c.Start()

	// Run indefinitely
	select {}

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
	// TODO TMP function: will be removed once Kosio is ready with the sensor Ranks
	// select the active sensors from redis and attach dummy Ranks
	dummyAssignScoreToEachActiveSensor()

	// Retrieve the first X records with the highest rank
	chosenSensorsRedis, err := redisClient.ZRevRangeWithScores(constants.RedisActiveSensorsRankKey, 0, int64(numberOfSensors-1)).Result()
	if err != nil {
		err = fmt.Errorf("ZRevRangeWithScores err:%v", err)
		return
	}

	if len(chosenSensorsRedis) == 0 {
		err = fmt.Errorf("No active sensors with populated rate")
		return
	}

	var chosenSensorsIds []uuid.UUID

	// Iterate over the results
	for _, sensor := range chosenSensorsRedis {
		sensorId := sensor.Member.(string)
		sensorScore := sensor.Score

		var sid uuid.UUID
		sid, err = uuid.FromBytes([]byte(sensorId))

		schedulerLogger.Info(fmt.Sprintf("ChosenSensor sensorId: %v, Rank: %.2f\n, numberOfSensors:%v", sid.String(), sensorScore, numberOfSensors))

		if err != nil {
			err = fmt.Errorf("can not convert sensorId from redis to uuid:%v err:%v", sensorId, err)
			return
		}
		chosenSensorsIds = append(chosenSensorsIds, sid)
	}

	dbResult := gormClient.Where("id IN (?)", chosenSensorsIds).Find(&sensors)
	if dbResult.Error != nil {
		err = fmt.Errorf("db error while getting chosenSensorsIds err:%v", err)
		return
	}

	return
}

// TODO TMP function: will be removed once Kosio is ready with the sensor Ranks
// select the active sensors from redis and attach dummy Ranks
func dummyAssignScoreToEachActiveSensor() {

	// Fetch all keys that match the prefix
	keys, err := redisClient.Keys(constants.RedisActiveSensorsKeyPrefix + "*").Result()
	if err != nil {
		fmt.Printf("dummyAssignScoreToEachActiveSensor: failed to retrieve keys from Redis: %v \n", err)
		return
	}

	schedulerLogger.Info("Currently active sensors:", keys)

	// delete all currenty ranked sensors
	_, err = redisClient.ZRemRangeByRank(constants.RedisActiveSensorsRankKey, 0, -1).Result()
	if err != nil {
		fmt.Println("redisClient.ZRemRangeByRank delete Error:", err)
		return
	}

	// Iterate over the keys and fetch their corresponding values
	for _, key := range keys {
		sensor_id, err := redisClient.Get(key).Result()
		if err != nil {
			fmt.Printf("dummyAssignScoreToEachActiveSensor: Failed to retrieve value for key %s: %v\n", key, err)
			continue
		}

		// Add a member with a score to the sorted set.
		err = redisClient.ZAdd(constants.RedisActiveSensorsRankKey, redis.Z{Score: rand.Float64(), Member: sensor_id}).Err()
		if err != nil {
			fmt.Println("redisClient.ZAdd Error:", err)
			return
		}
	}

	// // Set TTL for the sorted set key.
	// err = redisClient.Expire(constants.RedisActiveSensorsRankKey, 5*time.Minute).Err()
	// if err != nil {
	// 	fmt.Println("Error setting TTL:", err)
	// 	return
	// }
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
