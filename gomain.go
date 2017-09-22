package main

import (
	"github.com/BinArchitecture/GoRocketmqSender/rocketmq"
	"flag"
	"fmt"
	"encoding/json"
	"runtime"
	//"strconv"
	"time"
	//"git.apache.org/thrift.git/lib/go/thrift"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	producer, err := rocketmq.NewDefaultProducer("test1Group", "10.6.30.109:9876","prodInstance")
	if err != nil {
		panic(err)
	}
	prod,er:=rocketmq.NewRoutingProducer(producer,50000)
	if er != nil {
		panic(er)
	}
	prod.Start()
	size:=500000
	results := make(chan *rocketmq.SendResult,50000)
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
	props := make(map[string]string)
	props["fuck"] = "asd"
	//d := strconv.Itoa(j)
	//d = d + "大神哈哈fuck"
	//body := []byte(d)
	body := []byte("大神哈哈fuck")
	msg := &rocketmq.Message{"mqfuck", 0, props, body}
		for j := 1; j <= size; j++ {
			go func() {
				result,_:=handleSendMq(prod,msg)
				results<-result
			}()
		}
		time.Sleep(5*time.Second)

	for{
		time.Sleep(time.Second)
		fmt.Printf("resultLen:%d\n",len(results))
	}
}

func handleSendMq(prod rocketmq.Producer,msg *rocketmq.Message) (*rocketmq.SendResult,error){
	result,err:=prod.Send(msg)
	return result,err
}