package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ping-42/42lib/db/models"
	"github.com/stretchr/testify/assert"
)

type rankTestCase struct {
	name           string
	input          RankerData
	expectedOutput []models.SensorRank
}

var testCases = []rankTestCase{
	{
		name: "test success for N sensors",
		input: RankerData{
			RuntimeStats: []models.TsHostRuntimeStat{
				{
					SensorID:       id[1],
					CpuUsage:       10,
					MemUsedPercent: 10,
					GoRoutineCount: 2,
				},
				{
					SensorID:       id[2],
					CpuUsage:       20,
					MemUsedPercent: 20,
					GoRoutineCount: 2,
				},
				{
					SensorID:       id[3],
					CpuUsage:       50,
					MemUsedPercent: 50,
					GoRoutineCount: 16,
				},
			},
			LastSensorTasks: []models.Task{
				{
					SensorID:  id[1],
					CreatedAt: time.Now().Add(-10 * time.Minute),
				},
				{
					SensorID:  id[2],
					CreatedAt: time.Now().Add(-20 * time.Minute),
				},
				{
					SensorID:  id[3],
					CreatedAt: time.Now().Add(-30 * time.Minute),
				},
			},
		},
		expectedOutput: []models.SensorRank{
			{
				SensorID:         id[1],
				Rank:             91.39393939393939,
				DistributionRank: 10,
			},
			{
				SensorID:         id[2],
				Rank:             83.39393939393939,
				DistributionRank: 20,
			},
			{
				SensorID:         id[3],
				Rank:             55.15151515151515,
				DistributionRank: 30,
			},
		},
	},
	{
		name: "full capacity sensor should be skipped",
		input: RankerData{
			RuntimeStats: []models.TsHostRuntimeStat{
				{
					SensorID:       id[1],
					CpuUsage:       100,
					MemUsedPercent: 10,
					GoRoutineCount: 2,
				},
			},
			LastSensorTasks: []models.Task{
				{
					SensorID:  id[1],
					CreatedAt: time.Now().Add(-10 * time.Minute),
				},
			},
		},
		expectedOutput: []models.SensorRank{
			{
				SensorID:         id[1],
				Rank:             0,
				DistributionRank: 10,
			},
		},
	},
	{
		name: "missmatched data should be ok",
		input: RankerData{
			RuntimeStats: []models.TsHostRuntimeStat{
				{
					SensorID:       id[1],
					CpuUsage:       10,
					MemUsedPercent: 10,
					GoRoutineCount: 2,
				},
			},
			LastSensorTasks: []models.Task{
				{
					SensorID:  id[2],
					CreatedAt: time.Now().Add(-10 * time.Minute),
				},
			},
		},
		expectedOutput: []models.SensorRank{
			{
				SensorID:         id[1],
				Rank:             91.39393939393939,
				DistributionRank: 0,
			},
		},
	},
	{
		name: "empty data should give empty result",
		input: RankerData{
			LastSensorTasks: []models.Task{
				{
					SensorID:  id[2],
					CreatedAt: time.Now().Add(-10 * time.Minute),
				},
			},
		},
		expectedOutput: []models.SensorRank{},
	},
}

func Test_Rank(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t2 *testing.T) {
			results := getSensorRanks(tc.input)
			assert.Equal(t2, len(results), len(tc.expectedOutput), "result length")
			for i, actual := range results {
				assert.Equal(t2, tc.expectedOutput[i].SensorID, actual.SensorID, "sensor_id missmatch")
				assert.Equal(t2, tc.expectedOutput[i].Rank, actual.Rank, "rank missmatch")

				// dist rank is based on time calculations,
				// so the actual number has varying decimal value - assert without decimals
				actualDistRank := fmt.Sprintf("%.0f", actual.DistributionRank)
				expectedDistRank := fmt.Sprintf("%.0f", tc.expectedOutput[i].DistributionRank)
				assert.Equal(t2, expectedDistRank, actualDistRank, "distribution_rank missmatch")
			}
		})
	}
}

var id = map[int]uuid.UUID{
	1: uuid.MustParse("3f8f0e4d-6723-4d52-a1b8-8a203af94765"),
	2: uuid.MustParse("352e751c-5c7d-411a-9c9c-9a9a036fccb3"),
	3: uuid.MustParse("afb51543-b7c7-4324-9fe9-46a20295a50a"),
	4: uuid.MustParse("4f1ddde4-f9af-4fda-8f8e-2f381ab06e58"),
	5: uuid.MustParse("ea173492-71ef-4104-92c8-f4441960a41a"),
	6: uuid.MustParse("0fcbf6f0-51ee-4b97-8375-dc56a671858f"),
	7: uuid.MustParse("a427c4ac-b594-4d06-a18c-906f0dd901b3"),
	8: uuid.MustParse("d87b5cc4-de8f-4159-ab52-8756b32ad49b"),
	9: uuid.MustParse("4c13261e-2f33-4143-8bfd-65f3802e82af"),
}
