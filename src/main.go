package main

import (
	"model"
	"sync"
	"Kafka"
)

func main() {
	waitGroup := sync.WaitGroup{}
	Kafka.SendMsgIntoKafka(waitGroup, "", "")
	go model.QueryData(waitGroup, "sales_flat_order", 1, model.BA_PARAMTER)
	go model.QueryData(waitGroup, "sales_flat_order", 2, model.FU_PARAMTER)
	waitGroup.Wait()
}
