package main

import (
	"github.com/containerd/log"
	"github.com/ping-42/scheduler/ranker"
	"github.com/ping-42/scheduler/scheduler"

	"github.com/ping-42/42lib/config"
	"github.com/ping-42/42lib/db"
	"github.com/ping-42/42lib/logger"
)

// TODO: add these as config?
const (
	schedulerIntervalMinutes  = 1
	rankWorkerIntervalMinutes = 10
)

// Release versioning magic
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

var schedulerLogger = logger.Base("scheduler")
var configuration config.Configuration

func main() {
	schedulerLogger.WithFields(log.Fields{
		"version":   version,
		"commit":    commit,
		"buildDate": date,
	}).Info("Starting PING42 Scheduler Service ...")

	configuration = config.GetConfig()

	gormClient, err := db.InitPostgreeDatabase(configuration.PostgreeDBDsn)
	if err != nil {
		schedulerLogger.Error("error while InitPostgreeDatabase()", err.Error())
		panic(err.Error())
	}

	redisClient, err := db.InitRedis(configuration.RedisHost, configuration.RedisPassword)
	if err != nil {
		schedulerLogger.Error("error while InitRedis()", err.Error())
		panic(err.Error())
	}

	go ranker.Work(rankWorkerIntervalMinutes, redisClient, gormClient, schedulerLogger.WithField("unit", "sensorRank"))

	scheduler.Work(schedulerIntervalMinutes, redisClient, gormClient, schedulerLogger)
}
