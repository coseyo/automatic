package automatic

import (
	"errors"

	"github.com/coseyo/goutil/slog"

	"fmt"

	"github.com/coseyo/uniqueue"
)

type Job struct {
	Name string
	// 生产者函数
	Producer ProduceFunc

	// 锁定秒数
	LockSeconds int

	// 消费者函数
	Worker func([]byte) error

	// 并发工作数目
	WorkerNum int

	ProducerParams map[string]interface{}

	// cronjob param "@every 3h"
	Spec string

	// 队列对象，初始化时自动生成
	queue *uniqueue.Queue
}

// PushQueue 有加锁处理，防止短时间内有没处理完的任务重复入队
func (j *Job) PushQueue(lockKey string, data map[string]interface{}) (err error) {
	lockKey = fmt.Sprintf("%s_%s", j.Name, lockKey)
	if j.LockSeconds > 0 {
		err = j.queue.LockAndPush(lockKey, j.LockSeconds, data)
	} else {
		err = j.queue.Push(data)
	}

	if err != nil {
		err = errors.New("PushQueue err: " + err.Error())
	}

	slog.SimpleLog(j.Name, "PushQueue", map[string]interface{}{
		"lockKey": lockKey,
		"data":    data,
		"err":     err,
	})

	return
}

// Run cronjob定时调用
func (j *Job) Run() {
	if j.LockSeconds > 0 {
		if ok, _ := uniqueue.Lock(j.Name, j.LockSeconds, j.LockSeconds); !ok {
			return
		}
	}

	err := j.Producer(j, j.ProducerParams)

	if err != nil {
		slog.SimpleLog(j.Name, "Produce", map[string]interface{}{
			"name": j.Name,
			"err":  err,
		})
	}
}
