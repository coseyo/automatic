package auto_repay

import (
	"time"

	"vas_libs/automatic"
)

// 生成需要处理的数据
func producerOrders(job *automatic.Job, params map[string]interface{}) (err error) {
	limit := 1000
	offset := 0
	sleepTime := 1 * time.Second
	orders := []models.RepayOrder{}
	for {
		orders, err = models.VRepayOrderModel.GetOrders(models.REPAY_ORDER_STATUS_UNCHECKED, limit, offset)
		if err != nil || len(orders) == 0 {
			break
		}

		for _, v := range orders {
			job.PushQueue(v.Orderid, map[string]interface{}{
				"userid":   v.Userid,
				"order_id": v.Orderid,
			})
		}
		time.Sleep(sleepTime)
	}

	return
}
