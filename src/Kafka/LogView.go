package Kafka

import (
	"path/filepath"
	"os"
	"io/ioutil"
	"encoding/json"
	"sync"
	"log"
	"github.com/Shopify/sarama"
	"fmt"
	"strings"
	"time"
)

type SSlog struct {
	WebsiteId               int64   `json:"website_id"`
	DistinctId              string  `json:"distinct_id"`
	PromotionSource         string  `json:"promotion_source"`
	PlatformId              int64   `json:"platform_id"`
	Date                    string  `json:"date"`
	WTagPage                string  `json:"w_tag_page"`
	Time                    string  `json:"time"`
	CouponIdList            string  `json:"coupon_id_list"`
	Model                   string  `json:"model"`
	Browser                 string  `json:"browser"`
	Country                 string  `json:"country"`
	WTagHyperlink           string  `json:"w_tag_hyperlink"`
	Ip                      string  `json:"ip"`
	ScreenHeight            string  `json:"screen_height"`
	ScreenWidth             string  `json:"screen_width"`
	ITagHyperlink           string  `json:"i_tag_hyperlink"`
	GrandTotal              float64 `json:"grand_total"`
	Referrer                string  `json:"referrer"`
	Title                   string  `json:"title"`
	LatestTrafficSourceType string  `json:"latest_traffic_source_type"`
	ITagPage                string  `json:"i_tag_page"`
	OsVersion               string  `json:"os_version"`
	Manufacturer            string  `json:"manufacturer"`
	Os                      string  `json:"os"`
	Sku                     string  `json:"sku"`
	ITag                    string  `json:"i_tag"`
	BrowserVersion          string  `json:"browser_version"`
	WTag                    string  `json:"w_tag"`
	Province                string  `json:"province"`
	LoginUserId             string  `json:"login_user_id"`
	City                    string  `json:"city"`
}

type MsgLog []*SSlog

var
(
	logger   = log.New(os.Stderr, "[sarama]", log.LstdFlags)
	location = []string{"192.168.236.11:9092", "192.168.236.12:9092", "192.168.236.13:9092"}
)

const (
	TOPIC_LOG     = "log_msg"
	TOPIC_PROFILE = "order_info"
)

//read local log info
func SendMsgIntoKafka(wGroup sync.WaitGroup, dir string, msg string) {
	if dir == "" {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if !strings.Contains(path, ".json") {
				return nil
			}
			wGroup.Add(1)
			go openRead(wGroup, path)
			wGroup.Wait()
			return nil
		})
	} else {
		wGroup.Add(1)
		go kafkaAsyncView(wGroup, nil, msg)
		wGroup.Wait()
	}
}

func openRead(wGroup sync.WaitGroup, path string) {
	msg := MsgLog{}
	source, err1 := ioutil.ReadFile(path)
	if err1 == nil {
		json.Unmarshal(source, &msg)
		//KafkaSynView(msg)
		kafkaAsyncView(wGroup, msg, "")
	}
}

//send msg async into  kafka
func kafkaAsyncView(wGroup sync.WaitGroup, data MsgLog, msg string) {
	defer wGroup.Done()
	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.ClientID = "test"
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	pro, e := sarama.NewAsyncProducer(location, config)
	if e != nil {
		panic(e)
	}
	defer pro.AsyncClose()
	if msg == "" {
		for i := 0; i < len(data); i++ {
			d1 := data[i]
			d2, _ := json.Marshal(d1)
			sendMsg(d2, pro, TOPIC_LOG)
		}
	} else {
		sendMsg([]byte(msg), pro, TOPIC_PROFILE)
	}
}
func sendMsg(d2 []byte, pro sarama.AsyncProducer, topic string) {
	//every return is new msg
	msg := sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Metadata = "this is a msg log"
	msg.Timestamp = time.Now()
	msg.Key = sarama.StringEncoder("key")
	msg.Value = sarama.ByteEncoder(d2)
	pro.Input() <- &msg
	select {
	case success := <-pro.Successes():
		fmt.Println(success)
	case fail := <-pro.Errors():
		fmt.Println(fail)
	}
}

//同步消息
func KafkaSynView(data MsgLog) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	for i := 0; i <= len(data); i++ {
		msg := sarama.ProducerMessage{}
		msg.Topic = TOPIC_LOG
		msg.Offset = sarama.OffsetNewest
		m1 := data[i]
		m2, _ := json.Marshal(m1)
		fmt.Println(string(m2))
		msg.Key = sarama.StringEncoder("logs")
		msg.Value = sarama.ByteEncoder(m2)
		producer, _ := sarama.NewSyncProducer(location, config)
		//producer, e := sarama.NewAsyncProducer(location,config)
		producer.SendMessage(&msg)
	}
}
