package automatic

import (
	"time"

	"github.com/coseyo/goutil/slog"

	"errors"

	"github.com/coseyo/uniqueue"
	"github.com/robfig/cron"
)

const (
	checkQueueInterval = 10
	jobRetryTimes      = 2
	jobRetryDelay      = 10
	jobFuncTimeout     = 90
	gcCycleTime        = 10
	jobLifetime        = 3600
)

// 第一个参数是job对象，第二个参数是produce函数用到的参数map，可以在调用时传入
type ProduceFunc func(*Job, map[string]interface{}) error

type Automatic struct {
	// basic param
	Name         string
	LogDirPrefix string
	Jobs         []*Job

	// redis param
	RedisHost          string
	RedisPoolSize      int
	RedisClientTimeout int

	// 队列为空的情况下，检查队列的时间间隔(秒)
	CheckQueueInterval,

	// 任务重试次数，重试时的延迟时间(秒)，任务执行的超时时间(秒)
	JobRetryTimes,
	JobRetryDelay,
	JobFuncTimeout,

	// 队列垃圾回收检测周期(秒)
	GCCycleTime,

	// 队列任务的生存时间(秒)
	JobLifetime int
}

func (am *Automatic) Init() (err error) {
	slog.LogPrefix = am.LogDirPrefix
	if err = am.initQueue(); err != nil {
		panic(err)
		return
	}

	if err = am.initCron(); err != nil {
		return
	}

	return
}

func (am *Automatic) initQueue() (err error) {
	uniqueue.SetLogPrefix(am.LogDirPrefix)
	if err = uniqueue.InitRedis("tcp", am.RedisHost, am.RedisPoolSize, time.Duration(am.RedisClientTimeout)); err != nil {
		return
	}

	dispatcher := &uniqueue.Dispatcher{am.Name, time.Duration(am.CheckQueueInterval), am.JobRetryTimes, time.Duration(am.JobRetryDelay), time.Duration(am.JobFuncTimeout)}
	for k, q := range am.Jobs {
		am.Jobs[k].queue = uniqueue.New(q.Name)
		dispatcher.DoBackground(am.Jobs[k].queue, q.WorkerNum, q.Worker)
		dispatcher.RunGC(am.Jobs[k].queue, time.Duration(am.GCCycleTime), time.Duration(am.JobLifetime))
	}

	return
}

func (am *Automatic) initCron() (err error) {
	var (
		isInitCron bool
		cr         *cron.Cron
	)
	for _, q := range am.Jobs {
		if q.Spec != "" {
			if !isInitCron {
				cr = cron.New()
				isInitCron = true
			}

			err = cr.AddJob(q.Spec, q)
		}

	}

	if isInitCron {
		cr.Start()
	}

	return
}

// Produce 生产指定任务数据
func (am *Automatic) Produce(name string, params map[string]interface{}) (err error) {
	var f ProduceFunc
	var j *Job

	for _, job := range am.Jobs {
		if name == job.Name {
			f = job.Producer
			j = job
			break
		}
	}

	if j.Name == "" {
		err = errors.New("empty job")
		return
	}

	go func() {
		if err := f(j, params); err != nil {
			slog.SimpleLog(name, "ERROR", "Produce", err)
		}
	}()
	return
}

// New 以默认值生成对象
func New(name, logDirPrefix string, jobs []*Job, redisHost string, redisPoolSize, redisClientTimeout int) *Automatic {
	return &Automatic{
		Name:               name,
		LogDirPrefix:       logDirPrefix,
		Jobs:               jobs,
		CheckQueueInterval: checkQueueInterval,
		JobRetryTimes:      jobRetryTimes,
		JobRetryDelay:      jobRetryDelay,
		JobFuncTimeout:     jobFuncTimeout,
		GCCycleTime:        gcCycleTime,
		JobLifetime:        jobLifetime,
		RedisHost:          redisHost,
		RedisPoolSize:      redisPoolSize,
		RedisClientTimeout: redisClientTimeout,
	}
}
