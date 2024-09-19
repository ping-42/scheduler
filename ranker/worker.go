package ranker

import (
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/ping-42/42lib/constants"
	"github.com/ping-42/42lib/db/models"
	"github.com/ping-42/42lib/logger"
	"github.com/ping-42/42lib/ranker"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	// distributionMultiplier (dm) increases dist rank as follows:
	// dm * <minutes from last test>
	distributionMultiplier = 1
)

// RankerData holds everything needed to calculate a sensor rank
type RankerData struct {
	RuntimeStats    []models.TsHostRuntimeStat
	LastSensorTasks []models.Task
	rankLogger      *logrus.Entry
}

func Work(minuteInterval time.Duration, redisClient *redis.Client, dbClient *gorm.DB, logger *logrus.Entry) {
	assignSensorScores(redisClient, dbClient, logger, int(minuteInterval))
	ticker := time.NewTicker(minuteInterval * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		assignSensorScores(redisClient, dbClient, logger, int(minuteInterval))
	}
}

// assignSensorScores wraps the ranker logic - get data, calculate rank and insert in the DB
func assignSensorScores(redisClient *redis.Client, dbClient *gorm.DB, rankLogger *logrus.Entry, interval int) {

	rankLogger.Info("Ranking triggered...")

	opts, err := getRankerData(redisClient, dbClient, rankLogger, interval)
	if err != nil {
		rankLogger.Errorf("getRankerData error: %v", err)
		return
	}
	opts.rankLogger = rankLogger

	ranks := getSensorRanks(opts)
	if len(ranks) == 0 {
		rankLogger.Warnf("got empty rank scores, skipping rank")
		return
	}

	if err := dbClient.Create(&ranks).Error; err != nil {
		rankLogger.Errorf("error inserting ranks to the DB: %v", err)
	}
}

func getRankerData(redisClient *redis.Client, dbClient *gorm.DB, rankLogger *logrus.Entry, interval int) (data RankerData, err error) {
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

	data.RuntimeStats, err = models.GetRuntimeStats(dbClient, interval, activeSensors)
	if err != nil {
		rankLogger.Errorf("failed to get runtime stats: %v", err)
		return
	}

	data.LastSensorTasks, err = models.GetLatestSensorTasks(dbClient, activeSensors)

	return
}

func getSensorRanks(opts RankerData) []models.SensorRank {
	if opts.rankLogger == nil {
		opts.rankLogger = logger.Base("scheduler").WithField("unit", "sensorRank")
	}

	// use an envelope and add all ranks to it
	sensorRanks := addRuntimeRank(nil, opts.RuntimeStats, nil)
	sensorRanks = addDistributionRank(sensorRanks, opts.LastSensorTasks, opts.rankLogger)

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
func addDistributionRank(sensorRanks map[string]ranker.RankEnvelope, data []models.Task, rankLogger *logrus.Entry) map[string]ranker.RankEnvelope {
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
