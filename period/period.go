package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

const (
	TypeHeartBeat = "system:heartbeat"
)

type HeartBeat struct {
	UserID int `json:"user_id"`
}

//----------------------------------------------
// 建立一個 task
// task 須包含 type & payload, 並被封裝在 asynq.Task struct 中
//----------------------------------------------

// HeartBeatTask payload.
func HeartBeatTask(userID int) (*asynq.Task, error) {
	// Specify task payload.
	payload, err := json.Marshal(HeartBeat{UserID: userID})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	// Return a new task with given type and payload.
	return asynq.NewTask(TypeHeartBeat, payload), nil
}

//---------------------------------------------------------------
// 建立 handle task 來處理進來的 input
// 注意須符合 asynq.HandlerFunc 的 interface
// 當 task 被觸發時, 由 handler 來處理我們的商業邏輯
//---------------------------------------------------------------
// HandleHeartBeatTask handler.
func HandleHeartBeatTask(c context.Context, t *asynq.Task) error {
	// Get user ID from given task.
	var heartBeat HeartBeat
	if err := json.Unmarshal(t.Payload(), &heartBeat); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	time := time.Now()
	// Do something you want
	log.Printf("[Task Type: %v] Sending Email to User: user_id=%d, now is %v", t.Type(), heartBeat.UserID, time)
	return nil
}

// BeatProducer function
func BeatProducer() {
	scheduler := asynq.NewScheduler(
		asynq.RedisClientOpt{
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		},
		&asynq.SchedulerOpts{},
	)
	task1, err := HeartBeatTask(1)
	if err != nil {
		log.Fatal(err)
	}
	task2, err := HeartBeatTask(2)
	if err != nil {
		log.Fatal(err)
	}

	// You can use cron spec string to specify the schedule.
	// The cron parameter is (minute, hour, day, month, day of week)
	entryID, err := scheduler.Register("* * * * *", task1, asynq.Queue("critical"))
	if err != nil {
		log.Fatal(err)
	}

	// 定时任务最小单位为分钟, 如果想要精确到秒的话可以使用 every 关键字
	log.Printf("registered an every minute entry: %q\n", entryID)
	// You can use "@every <duration>" to specify the interval.
	entryID, err = scheduler.Register("@every 60s", task2, asynq.Queue("default"))
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("registered an every 60s entry: %q\n", entryID)
	if err := scheduler.Run(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	BeatProducer()
}
