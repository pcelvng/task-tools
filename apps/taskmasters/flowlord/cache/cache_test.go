package cache

import (
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func TestAdd(t *testing.T) {
	fn := func(tasks []task.Task) (map[string]TaskJob, error) {
		cache := &Memory{cache: make(map[string]TaskJob)}
		for _, t := range tasks {
			cache.Add(t)
		}
		for k, v := range cache.cache {
			v.count = len(v.Events)
			v.Events = nil
			cache.cache[k] = v
		}
		return cache.cache, nil
	}
	cases := trial.Cases[[]task.Task, map[string]TaskJob]{
		"no id": {
			Input: []task.Task{
				{Type: "test"},
			},
		},
		"created": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.TimeDay("2023-01-01"),
					count:      1,
				},
			},
		},
		"completed": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.CompleteResult},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:00:01Z"),
					Completed:  true,
					count:      2,
				},
			},
		},
		"failed": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.ErrResult, Msg: "Error with pull from X"},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:00:01Z"),
					Completed:  true,
					count:      2,
				},
			},
		},
		"retry": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.ErrResult, Msg: "Error with pull from X"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:01:00Z", Meta: "retry=1"},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:01:00Z"),
					Completed:  false,
					count:      3,
				},
			},
		},
		"child": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Created: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.CompleteResult},
				{Type: "transform", ID: "id1", Info: "/product/2023-01-01/data.txt", Created: "2023-01-01T00:02:00Z"},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:02:00Z"),
					Completed:  false,
					count:      3,
				},
			},
		},
		"multi-child": {
			Input: []task.Task{
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Started: "2023-01-01T00:00:00Z"},
				{Type: "pull", ID: "id1", Info: "?date=2023-01-01", Started: "2023-01-01T00:00:00Z", Ended: "2023-01-01T00:00:01Z", Result: task.CompleteResult},
				{Type: "transform", ID: "id1", Info: "/product/2023-01-01/data.txt", Started: "2023-01-01T00:02:00Z"},
				{Type: "transform", ID: "id1", Info: "/product/2023-01-01/data.txt", Started: "2023-01-01T00:02:00Z", Ended: "2023-01-01T00:02:15Z", Result: task.CompleteResult},
				{Type: "load", ID: "id1", Info: "/product/2023-01-01/data.txt?table=schema.product", Started: "2023-01-01T00:04:00Z"},
				{Type: "load", ID: "id1", Info: "/product/2023-01-01/data.txt?table=schema.product", Started: "2023-01-01T00:04:00Z", Ended: "2023-01-01T00:05:12Z", Result: task.CompleteResult},
			},
			Expected: map[string]TaskJob{
				"id1": {
					LastUpdate: trial.Time(time.RFC3339, "2023-01-01T00:05:12Z"),
					Completed:  true,
					count:      6,
				},
			},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestRecycle(t *testing.T) {
	now := time.Now()
	cache := Memory{
		ttl_Minute: 60, // 1 hour
		cache: map[string]TaskJob{
			"keep": {
				Completed:  false,
				LastUpdate: now.Add(-30 * time.Minute),
				Events:     []task.Task{{Type: "test1"}},
			},
			"expire": {
				Completed:  true,
				LastUpdate: now.Add(-90 * time.Minute),
			},
			"not-completed": {
				Completed:  false,
				LastUpdate: now.Add(-90 * time.Minute),
				Events: []task.Task{
					{Type: "test1", Created: now.String()},
					{Type: "test1", Created: now.String(), Result: task.CompleteResult},
					{Type: "test2", Created: now.String()},
				},
			},
		},
	}

	stat := cache.Recycle()
	stat.ProcessTime = 0
	expected := Stat{
		Count:   1,
		Removed: 2,
		Unfinished: []task.Task{
			{Type: "test2", Created: now.String()},
		}}
	if eq, diff := trial.Equal(stat, expected); !eq {
		t.Logf(diff)
	}
}
