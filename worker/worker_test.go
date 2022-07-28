package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

// go test -v
// 一个封装任务创建和任务处理的包

var c *asynq.Client

func TestMain(m *testing.M) {
	r := asynq.RedisClientOpt{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}
	c = asynq.NewClient(r)
	ret := m.Run()
	c.Close()
	os.Exit(ret)
}

// 即时消费
func Test_Enqueue(t *testing.T) {
	payload, err := json.Marshal(EmailDelivery{UserID: 1, Message: "i'm immediately message"})
	if err != nil {
		panic(err)
	}

	// 立即处理任务
	task := asynq.NewTask("msg", payload)
	res, err := c.Enqueue(task)
	if err != nil {
		t.Errorf("could not enqueue task: %v", err)
		t.FailNow()
	}
	fmt.Printf("Enqueued Result: %+v\n", res)
}

// 延时消费
func Test_EnqueueDelay(t *testing.T) {
	payload, err := json.Marshal(EmailDelivery{UserID: 1, Message: "i'm delay 5 seconds message"})
	if err != nil {
		panic(err)
	}

	task := asynq.NewTask("msg", payload)
	// 延时处理任务， 5 秒时后处理
	res, err := c.Enqueue(task, asynq.ProcessIn(5*time.Second))
	// res, err := c.Enqueue(task, asynq.ProcessAt(time.Now().Add(5*time.Second)))
	if err != nil {
		t.Errorf("could not enqueue task: %v", err)
		t.FailNow()
	}
	fmt.Printf("Enqueued Result: %+v\n", res)
}

// 超时、重试、过期
func Test_EnqueueOther(t *testing.T) {
	payload, err := json.Marshal(EmailDelivery{UserID: 1, Message: "i'm delay 5 seconds message"})
	if err != nil {
		panic(err)
	}

	task := asynq.NewTask("msg", payload)
	// 任务重试，10秒超时，最多重试 3 次，20 秒后过期
	res, err := c.Enqueue(task, asynq.MaxRetry(3), asynq.Timeout(10*time.Second), asynq.Deadline(time.Now().Add(20*time.Second)))
	if err != nil {
		t.Errorf("could not enqueue task: %v", err)
		t.FailNow()
	}
	fmt.Printf("Enqueued Result: %+v\n", res)
}
