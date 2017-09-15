package main


// 生成需要处理的数据
func producerOrders(job *automatic.Job, params map[string]interface{}) (err error) {
	limit := 1000
	offset := 0
	sleepTime := 1 * time.Second
	orders := []Order{}
	for {
		orders, err = GetOrders(REPAY_ORDER_STATUS_UNCHECKED, limit, offset)
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
