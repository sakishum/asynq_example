package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

const (
	TypeEmailDelivery = "email:deliver"
)

type EmailDelivery struct {
	UserID  int    `json:"user_id"`
	Message string `json:"message"`
}

func NewEmailDeliveryTask(userID int, message string) (*asynq.Task, error) {
	payload, err := json.Marshal(EmailDelivery{UserID: userID, Message: message})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	// 要创建任务，请使用 NewTask函数，并为任务 类型名称（task name） 和 执行任务所需的数据（payload）
	return asynq.NewTask(TypeEmailDelivery, payload), nil
}

// 用于 Client 将任务放入队列中
func EmailDeliveryTaskAdd() {
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer client.Close()
	// Process the task immediately in critical queue.
	task, err := NewEmailDeliveryTask(42, "some:template:id")
	if err != nil {
		log.Fatalf("could not create task: %v", err)
	}
	info, err := client.Enqueue(task, asynq.Queue("low"))
	if err != nil {
		log.Fatalf("could not enqueue task: %v", err)
	}
	log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)
}

func main() {
	// 分发异步任务
	for i := 0; i < 3; i++ {
		EmailDeliveryTaskAdd()
		time.Sleep(time.Second * 3)
	}
}
