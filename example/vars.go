package main


import (
	"github.com/coseyo/automatic"
)

const (
	queueLockedTime = 1800

	JobCheckRepayOrderName = "jobCheckRepayOrder"
)

var (
	ATMT         *automatic.Automatic
	logDirPrefix = "test"

	jobOrder = &automatic.Job{
		Name:        JobCheckRepayOrderName,
		Producer:    producerOrders,
		LockSeconds: queueLockedTime,
		Worker:      doOrderJob,
		WorkerNum:   100,
		Spec:        "@every 1h",
	}

)
