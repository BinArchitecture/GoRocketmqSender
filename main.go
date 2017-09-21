package main

import (
	"github.com/BinArchitecture/GoRocketmqSender/rocketmq"
	"flag"
	"fmt"
	"encoding/json"
	"runtime"
	"strconv"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	producer, err := rocketmq.NewDefaultProducer("test1Group", "10.6.30.109:9876","prodInstance")
	if err != nil {
		panic(err)
	}
	producer.Start()
	//var cc int32 = 0
	jobs := make(chan *rocketmq.Message,10000)
	results := make(chan *rocketmq.SendResult,10000)
	for w := 1; w <= 10000; w++ {
		go worker(producer,w, jobs, results)
	}
	size:=500000
	go func() {
		for {
			result := <-results
			jj, er := json.Marshal(result)
			if er == nil && jj != nil {
				//fmt.Printf("result:%s\n",string(jj))
			} else if err != nil {
				fmt.Errorf("err is:%v", err)
			}
		}
	}()

	for i:=0;i<4;i++{
		for j := 1; j <= size; j++ {
			props := make(map[string]string)
			props["fuck"] = "asd"
			d := strconv.Itoa(j)
			d = d + "大神哈哈fuck"
			body := []byte(d)
			msg := &rocketmq.Message{"mqfuck", 0, props, body}
			//fmt.Printf("joblen:%d\n",len(jobs))
			jobs <- msg
		}
		time.Sleep(5*time.Second)
	}

	for{
		time.Sleep(time.Second)
		fmt.Printf("joblen:%d\n",len(jobs))
	}
	//close(jobs)

}

func worker(producer rocketmq.Producer,id int, jobs <-chan *rocketmq.Message, results chan<- *rocketmq.SendResult) {
	for {
		msg := <- jobs
		fmt.Println("worker", id, "processing job", string(msg.Body))
		result,_:=producer.Send(msg)
		results <- result
	}
}
