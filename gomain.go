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
	prod,er:=rocketmq.NewRoutingProducer(producer,10000)
	if er != nil {
		panic(er)
	}
	prod.Start()
	size:=200000
	results := make(chan *rocketmq.SendResult,10000)
	go func() {
		for {
			result := <-results
			jj, er := json.Marshal(result)
			if er == nil && jj != nil {
				fmt.Printf("result:%s\n",string(jj))
			} else if err != nil {
				fmt.Errorf("err is:%v", err)
			}
		}
	}()

		for j := 1; j <= size; j++ {
			props := make(map[string]string)
			props["fuck"] = "asd"
			d := strconv.Itoa(j)
			d = d + "大神哈哈fuck"
			body := []byte(d)
			msg := &rocketmq.Message{"mqfuck", 0, props, body}
			result,_:=prod.Send(msg)
			results<-result
		}
		time.Sleep(5*time.Second)

	for{
		time.Sleep(time.Second)
		fmt.Printf("resultLen:%d\n",len(results))
	}

}