package main

import (
	"flag"
	"runtime"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/BinArchitecture/GoRocketmqSender/rmq"
	"github.com/golang/glog"
	"fmt"
	"time"
	//"sync"
	"github.com/BinArchitecture/GoRocketmqSender/rocketmq"
	"errors"
	"sync"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	addr:="10.6.30.109:7912"
	props := make(map[string]string)
	props["fuck"] = "asd"
	//d := strconv.Itoa(j)
	//d = d + "大神哈哈fuck"
	//body := []byte(d)
	body := []byte("大神哈哈fuck")
	msg := &rmq.RmqMessage{"mqfuck", 0, props, body}
	size:=300000
	clientPool,_:=buildClientPool(addr,5000,3000)
	//client,_:=buildClient(addr)
	//var errCount int32=0
	wg := sync.WaitGroup{}
	wg.Add(size)
	run := func(entity interface{}) (interface{}, error) {
		msg := entity.(*rmq.RmqMessage)
		ttport:=clientPool.Get()
		if ttport==nil || !ttport.IsOpen(){
			return nil,errors.New("can't get client")
		}
		pF := thrift.NewTBinaryProtocolFactoryDefault()
		client := rmq.NewRmqThriftProdServiceClientFactory(ttport, pF)
		result, err := client.Send(msg)
		if err != nil {
			glog.Error(err)
		}
		clientPool.Release(ttport)
		return result, err
	}
	goRoutingPool, _ := rocketmq.NewGoCoRoutingPool(4000, run)
	goRoutingPool.Start()
	for j:=0;j<size;j++{
		go func() {
			defer wg.Done()
			rmr,_:=goRoutingPool.Do(msg)
			if rmr==nil{
				fmt.Errorf("rmresult send Fail\n")
				return
			}
			rmresult:=rmr.(*rmq.RmqSendResult_)
			fmt.Printf("rmresult:%v\n",rmresult)
			if !rmresult.IsSendOK{
				fmt.Errorf("rmresult:%v send Fail\n",rmresult)
			}
		}()
	}
	wg.Wait()
	clientPool.Destory()
	//client.Transport.Close()
}

func buildClient(addr string) (*rmq.RmqThriftProdServiceClient,error){
	sock, err := thrift.NewTSocket(addr)
	if err != nil {
		glog.Errorf("thrift.NewTSocketTimeout(%s) error(%v)", addr, err)
		return nil,err
	}
	tF := thrift.NewTTransportFactory()
	pF := thrift.NewTBinaryProtocolFactoryDefault()
	ttport,_:=tF.GetTransport(sock)
	client := rmq.NewRmqThriftProdServiceClientFactory(ttport, pF)
	if err = client.Transport.Open(); err != nil {
		glog.Errorf("client.Transport.Open() error(%v)", err)
		return nil,err
	}
	return client,nil
}

func buildClientPool(addr string,poolSize int,minSize int) (*rmq.ThriftTransportPool,error){
	addrs:=make([]string,1)
	addrs[0]=addr
	return rmq.NewThriftTransportPool(30 * time.Second,poolSize,minSize,30,10*time.Second,addrs),nil
}
