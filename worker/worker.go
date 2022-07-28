package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"log"
	"os"
	"os/signal"

	"github.com/hibiken/asynq"
	"golang.org/x/sys/unix"
)

// TypeHeartBeat is a name of the task type
const (
	TypeHeartBeat     = "system:heartbeat"
	SyncAliYunCloud   = "cmdb:aliyun"
	TypeEmailDelivery = "email:deliver"
)

type HeartBeat struct {
	UserID int `json:"user_id"`
}

type EmailDelivery struct {
	UserID  int    `json:"user_id"`
	Message string `json:"message"`
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
	log.Printf("[Task Type: %v] HeartBeat: user_id=%d, now is %v", t.Type(), heartBeat.UserID, time)
	return nil
}

// 任务处理
func HandleEmailDeliveryTask(ctx context.Context, t *asynq.Task) error {
	var email EmailDelivery
	if err := json.Unmarshal(t.Payload(), &email); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	// 逻辑处理 start...
	//log.Printf("Aliyun Cloud assets are successfully synchronized...")
	log.Printf("[Task Type: %v] Sending Email to User: user_id=%d, message=%s", t.Type(), email.UserID, email.Message)
	return nil
}

// loggingMiddleware 记录任务日志中间件
func loggingMiddleware(h asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		start := time.Now()
		log.Printf("Start processing %q", t.Type())
		err := h.ProcessTask(ctx, t)
		if err != nil {
			return err
		}

		log.Printf("Finished processing %q: Elapsed Time = %v", t.Type(), time.Since(start))
		return nil
	})
}

// 异步任务服务入口文件
func main() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{
			Addr: "localhost:6379",
			// Omit if no password is required
			Password: "",
			// Use a dedicated db number for asynq.
			// By default, Redis offers 16 databases (0..15)
			DB: 0,
		},
		asynq.Config{
			// 每个进程并发执行的worker数量
			Concurrency: 20,
			// Optionally specify multiple queues with different priority.
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
			// See the godoc for other configuration options
		},
	)

	// 启动一个工作服务器以在后台处理这些任务。
	// 要启动 worker，使用 Server并提供 Handler来处理任务。
	// 可以选择使用 ServeMux来创建处理程序，就像使用 net/httpHandler 一样。
	mux := asynq.NewServeMux()
	// 加载中间件
	mux.Use(loggingMiddleware)
	// ...register other middleware...
	// 任务执行时的 handle
	mux.HandleFunc(TypeEmailDelivery, HandleEmailDeliveryTask)
	mux.HandleFunc(TypeHeartBeat, HandleHeartBeatTask)
	// ...register other handlers...

	// start server
	if err := srv.Start(mux); err != nil {
		log.Fatalf("could not start server: %v", err)
	}

	// Wait for termination signal.
	signs := make(chan os.Signal, 1)
	signal.Notify(signs, unix.SIGTERM, unix.SIGINT, unix.SIGTSTP)
	for {
		s := <-signs
		if s == unix.SIGTSTP {
			srv.Shutdown()
			continue
		}
		break
	}

	// Stop worker server.
	srv.Stop()
}
