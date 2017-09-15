package main

func doOrderJob(job []byte) (err error) {
	value := make(map[string]interface{})
	json.Unmarshal(job, &value)
	userid := convert.InterfaceToInt64(value["userid"])
	orderid := convert.InterfaceToString(value["order_id"])
	err = checkRepayOrder(userid, orderid)
	return
}

