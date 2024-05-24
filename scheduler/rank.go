package main

import (
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/ping-42/42lib/constants"
	"github.com/ping-42/42lib/db/models"
	"github.com/ping-42/42lib/ranker"
	"gorm.io/gorm"
)

const (
	rankWorkerIntervalMinutes = 10

	// distributionMultiplier (dm) increases dist rank as follows:
	// dm * <minutes from last test>
	distributionMultiplier = 1
)

var rankLogger = schedulerLogger.WithField("unit", "sensorRank")

// RankerData holds everything needed to calculate a sensor rank
type RankerData struct {
	RuntimeStats    []models.TsHostRuntimeStat
	LastSensorTasks []models.Task
}

// assignSensorScores wraps the ranker logic - get data, calculate rank and insert in the DB
func assignSensorScores(redisClient *redis.Client, dbClient *gorm.DB) {

	opts, err := getRankerData(redisClient, dbClient)
	if err != nil {
		rankLogger.Errorf("getRankerData error: %v", err)
		return
	}

	ranks := getSensorRanks(opts)
	if len(ranks) == 0 {
		rankLogger.Warnf("got empty rank scores, skipping rank")
		return
	}

	if err := dbClient.Create(&ranks).Error; err != nil {
		rankLogger.Errorf("error inserting ranks to the DB: %v", err)
	}
}

func getRankerData(redisClient *redis.Client, dbClient *gorm.DB) (data RankerData, err error) {
	activeSensors, err := redisClient.Keys(constants.RedisActiveSensorsKeyPrefix + "*").Result()
	if err != nil {
		rankLogger.Errorf("failed to retrieve keys from Redis: %v", err)
		return
	}

	// normalise values: [active_sensors<value>] -> [<value>]
	// TODO: there should be a way to skip this
	for k, v := range activeSensors {
		activeSensors[k] = strings.TrimPrefix(v, constants.RedisActiveSensorsKeyPrefix)
		if _, err = uuid.Parse(activeSensors[k]); err != nil {
			rankLogger.Errorf("failed to parse active sensor UUID from redis: %v, ranking aborted", activeSensors[k])
			return
		}
	}
	rankLogger.Info("Currently active sensors:", activeSensors)

	data.RuntimeStats, err = models.GetRuntimeStats(dbClient, rankWorkerIntervalMinutes, activeSensors)
	if err != nil {
		rankLogger.Errorf("failed to get runtime stats: %v", err)
	}

	data.LastSensorTasks, err = models.GetLatestSensorTasks(dbClient, activeSensors)
	if err != nil {
		return
	}

	return
}

func getSensorRanks(opts RankerData) []models.SensorRank {
	// use an envelope and add all ranks to it
	sensorRanks := addRuntimeRank(nil, opts.RuntimeStats, nil)
	sensorRanks = addDistributionRank(sensorRanks, opts.LastSensorTasks)

	// calculate final rank
	finalWeightGetter := func() interface{} { return ranker.DefaultFinalRankWeights }
	ranks := make([]models.SensorRank, len(sensorRanks))
	i := 0
	for sensorId, re := range sensorRanks {
		ranks[i] = models.SensorRank{
			SensorID:         uuid.MustParse(sensorId),
			Rank:             re.Rank(finalWeightGetter),
			DistributionRank: re.DistributionRank,
			CreatedAt:        time.Now(),
		}
		i++
	}

	return ranks
}

// addRuntimeRank adds the runtimeRank to the given sensorRanks map based of CPU, MEM and GoRoutine count
func addRuntimeRank(sensorRanks map[string]ranker.RankEnvelope, stats []models.TsHostRuntimeStat, weights *ranker.HostRuntimeWeights) map[string]ranker.RankEnvelope {
	if weights == nil {
		weights = &ranker.DefaultHostRuntimeWeights
	}
	weightGetter := func() interface{} {
		return *weights
	}
	if sensorRanks == nil {
		sensorRanks = make(map[string]ranker.RankEnvelope)
	}

	// append rank to current envelope or create a new
	for _, r := range stats {
		currentRank := r.Rank(weightGetter)

		if re, ok := sensorRanks[r.SensorID.String()]; ok {
			re.RuntimeRank = currentRank
			sensorRanks[r.SensorID.String()] = re
			continue
		}

		sensorRanks[r.SensorID.String()] = ranker.RankEnvelope{
			RuntimeRank: currentRank,
		}
	}

	return sensorRanks
}

// addDistributionRank adds the rank to the given map, to ensure sensor rotation
func addDistributionRank(sensorRanks map[string]ranker.RankEnvelope, data []models.Task) map[string]ranker.RankEnvelope {
	if sensorRanks == nil {
		sensorRanks = make(map[string]ranker.RankEnvelope)
	}
	now := time.Now()
	for _, task := range data {
		if re, ok := sensorRanks[task.SensorID.String()]; ok {
			durFromLastTask := distributionMultiplier * (now.Sub(task.CreatedAt))
			re.DistributionRank = durFromLastTask.Minutes()
			sensorRanks[task.SensorID.String()] = re
			continue
		}
		rankLogger.Debugf("got sensor task data, but no ranking, id: %v", task.SensorID.String())
	}

	return sensorRanks
}