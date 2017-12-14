package test
//
//import (
//	"github.com/BinArchitecture/GoRocketmqSender/rocketmq"
//	"flag"
//	"fmt"
//	"time"
//    //"runtime/pprof"
//	//"os"
//	//"github.com/golang/glog"
//	"encoding/json"
//	"runtime"
//	"sync/atomic"
//)
//
//func main() {
//	runtime.GOMAXPROCS(runtime.NumCPU())
//	flag.Parse()
//	producer, err := rocketmq.NewDefaultProducer("test1Group", "10.6.30.109:9876","prodInstance")
//	if err != nil {
//		panic(err)
//	}
//	producer.Start()
//	var cc int32 = 0
//	for i:=0;i<50;i++{
//		sendMsgConcurrently(err, producer,cc)
//		//time.Sleep(1*time.Second)
//	}
//}
//func sendMsgConcurrently(err error, producer rocketmq.Producer,cc int32) {
//	props := make(map[string]string)
//	props["fuck"] = "binfuck"
//	props["fuck1"] = "是的"
//	props["fuck2"] = "123岁"
//	body := []byte("大神哈哈fuck")
//	msg := &rocketmq.Message{"mqfuck", 0, props, body}
//	size := 10000
//	chanprod := make(chan *rocketmq.SendResult, 10000)
//	//chanprod:=make(chan bool,50000)
//	//f, err := os.OpenFile("/tmp/cpu.prof", os.O_RDWR|os.O_CREATE, 0644)
//	//if err != nil {
//	//	glog.Fatal(err)
//	//}
//	//defer f.Close()
//	//pprof.StartCPUProfile(f)
//	//defer pprof.StopCPUProfile()
//	var begin = time.Now().Unix()
//	for j := 0; j < size; j++ {
//		go func() {
//			result, err := producer.Send(msg)
//			if err != nil {
//				fmt.Errorf("error:%s", err.Error())
//			}
//			chanprod <- result
//			//invoke:=func(responseFuture *rocketmq.ResponseFuture){
//			//	fmt.Printf("succ%b\n",responseFuture.SendRequestOK)
//			//	if responseFuture!=nil{
//			//		chanprod<-responseFuture.SendRequestOK
//			//	}
//			//}
//			//producer.SendAsync(msg,invoke)
//		}()
//		//result,err:=producer.Send(msg)
//		//if err==nil{
//		//	jj,er:=json.Marshal(result)
//		//	if er==nil{
//		//		fmt.Printf("result:%s",string(jj))
//		//	}
//		//}
//		//invoke:=func(responseFuture *rocketmq.ResponseFuture){
//		//	fmt.Printf("succ%b\n",responseFuture.SendRequestOK)
//		//}
//		//producer.SendAsync(msg,invoke)
//	}
//	for {
//		result := <-chanprod
//		jj, er := json.Marshal(result)
//		if er == nil && jj != nil {
//			//fmt.Printf("result:%s\n",string(jj))
//			cc = atomic.AddInt32(&cc, 1)
//		} else if err != nil {
//			fmt.Errorf("err is:%v", err)
//		}
//		//if result{
//		//	cc=atomic.AddInt32(&cc,1)
//		//}
//		//fmt.Printf("cc is:%d\n", int(cc))
//		if int(cc) == size {
//			end := time.Now().Unix()
//			fmt.Printf("totllyCost:%ds\n", end-begin)
//			//pprof.StopCPUProfile()
//			//f.Close()
//			break
//		}
//	}
//}
