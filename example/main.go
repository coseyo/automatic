package main


func init() {
	initAutomatic()
}

func initAutomatic() {
	jobs := []*automatic.Job{
		jobOrder,
	}

	redisHost := "1.1.1.1"
	redisPoolsize := 30
	ATMT = automatic.New("test", "logDirPrefix", jobs, redisHost, redisPoolsize, 900)
	if err := ATMT.Init(); err != nil {
		beego.Error(err)
	}
}

