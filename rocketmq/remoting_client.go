package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"net"
	"sync"
	"time"
)

type InvokeCallback func(responseFuture *ResponseFuture)

type ResponseFuture struct {
	ResponseCommand *RemotingCommand
	SendRequestOK   bool
	err             error
	opaque          int32
	timeoutMillis   int64
	invokeCallback  InvokeCallback
	beginTimestamp  int64
	done            chan bool
}

type RemotingClient interface {
	connect(addr string) (net.Conn, error)
	invokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error
	invokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error)
	ScanResponseTable()
}

type DefalutRemotingClient struct {
	connTable          map[string]net.Conn
	connTableLock      sync.RWMutex
	responseTable      map[int32]*ResponseFuture
	responseTableLock  sync.RWMutex
	namesrvAddrList    []string
	namesrvAddrChoosed string
}

func NewDefaultRemotingClient() RemotingClient {
	return &DefalutRemotingClient{
		connTable:    make(map[string]net.Conn),
		responseTable: make(map[int32]*ResponseFuture),
	}
}

func (self *DefalutRemotingClient) ScanResponseTable() {
	self.responseTableLock.Lock()
	for seq, response := range self.responseTable {
		if  (response.beginTimestamp + 30) <= time.Now().Unix() {

			delete(self.responseTable, seq)

			if response.invokeCallback != nil {
				response.invokeCallback(nil)
				glog.Errorf("remove time out request %v", response)
			}
		}
	}
	self.responseTableLock.Unlock()

}

func (self *DefalutRemotingClient) connect(addr string) (conn net.Conn, err error) {
	if addr == "" {
		addr = self.namesrvAddrChoosed
	}

	self.connTableLock.RLock()
	conn, ok := self.connTable[addr]
	self.connTableLock.RUnlock()
	if !ok {
		self.connTableLock.Lock()
		if conn, ok := self.connTable[addr];ok{
			defer self.connTableLock.Unlock()
			return conn,nil
		}
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			glog.Error(err)
			return nil, err
		}
		//fmt.Printf("connect to:%s\n", addr)
		self.connTable[addr] = conn
		go self.handlerConn(conn, addr)
		glog.Infoln("start handelConn:", addr)
		glog.Infoln("connect to:", addr)
		self.connTableLock.Unlock()
	}

	return conn, nil
}

func (self *DefalutRemotingClient) invokeSync(addr string, request *RemotingCommand, timeoutMillis int64) (*RemotingCommand, error) {
	response := &ResponseFuture{
		SendRequestOK:  false,
		opaque:         request.Opaque,
		timeoutMillis:  timeoutMillis,
		beginTimestamp: time.Now().Unix(),
		done:           make(chan bool),
	}
	_,err:=self.invoke(addr,request,response)
	if err!=nil{
		glog.Error(err)
		return nil, err
	}
	select {
	case <-response.done:
		return response.ResponseCommand, nil
	case <-time.After(3 * time.Second):
		return nil, errors.New("invoke sync timeout")
	}

}

func (self *DefalutRemotingClient) invoke(addr string, request *RemotingCommand,response *ResponseFuture) (*RemotingCommand, error){
	conn, err := self.connect(addr)
	if err!=nil{
		glog.Error(err)
		return nil, err
	}
	header := request.encodeHeader()
	body := request.Body
	self.responseTableLock.Lock()
	self.responseTable[request.Opaque] = response
	self.responseTableLock.Unlock()
	err = self.sendRequest(header, body, conn, addr)
	if err != nil {
		glog.Error(err)
		return nil, err
	}
	return nil,nil
}

func (self *DefalutRemotingClient) invokeAsync(addr string, request *RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	response := &ResponseFuture{
		SendRequestOK:  false,
		opaque:         request.Opaque,
		timeoutMillis:  timeoutMillis,
		beginTimestamp: time.Now().Unix(),
		invokeCallback: invokeCallback,
	}
	_,err:=self.invoke(addr,request,response)
	if err!=nil{
		glog.Error(err)
		return err
	}
	return nil
}

func (self *DefalutRemotingClient) handlerConn(conn net.Conn, addr string) {
	b := make([]byte, 1024)
	var length, headerLength, bodyLength int32
	var buf = bytes.NewBuffer([]byte{})
	var header, body []byte
	var flag int = 0
	//var cf int32 =0
	for {
		n, err := conn.Read(b)
		if err != nil {
			glog.Error(err, addr)
			self.releaseConn(addr, conn)
			return
		}

		_, err = buf.Write(b[:n])
		if err != nil {
			glog.Error(err, addr)
			self.releaseConn(addr, conn)
			return
		}

		for {
			if flag == 0 {
				if buf.Len() >= 4 {
					err = binary.Read(buf, binary.BigEndian, &length)
					if err != nil {
						glog.Error(err)
						return
					}
					flag = 1
				} else {
					break
				}
			}

			if flag == 1 {
				if buf.Len() >= 4 {
					err = binary.Read(buf, binary.BigEndian, &headerLength)
					if err != nil {
						glog.Error(err)
						return
					}
					flag = 2
				} else {
					break
				}

			}

			if flag == 2 {
				if (buf.Len() > 0) && (buf.Len() >= int(headerLength)) {
					header = make([]byte, headerLength)
					_, err = buf.Read(header)
					if err != nil {
						glog.Error(err)
						return
					}
					flag = 3
				} else {
					break
				}
			}

			if flag == 3 {
				bodyLength = length - 4 - headerLength
				if bodyLength == 0 {
					flag = 0
				} else {

					if buf.Len() >= int(bodyLength) {
						body = make([]byte, int(bodyLength))
						_, err = buf.Read(body)
						if err != nil {
							glog.Error(err)
							return
						}
						flag = 0
					} else {
						break
					}
				}
			}

			if flag == 0 {
				headerCopy := make([]byte, len(header))
				bodyCopy := make([]byte, len(body))
				copy(headerCopy, header)
				copy(bodyCopy, body)
				go func() {
					cmd := decodeRemoteCommand(headerCopy, bodyCopy)
					self.responseTableLock.RLock()
					response, ok := self.responseTable[cmd.Opaque]
					self.responseTableLock.RUnlock()

					self.responseTableLock.Lock()
					delete(self.responseTable, cmd.Opaque)
					self.responseTableLock.Unlock()

					if ok {
						response.ResponseCommand = cmd
						if response.invokeCallback != nil {
							response.SendRequestOK=true
							//cf=atomic.AddInt32(&cf,1)
							response.invokeCallback(response)
							//fmt.Printf("invoke is %d\n",int(cf))
						}

						if response.done != nil {
							response.done <- true
						}
					} else {
						//if cmd.Code == NOTIFY_CONSUMER_IDS_CHANGED {
						//	return
						//}
						jsonCmd, err := json.Marshal(cmd)

						if err != nil {
							glog.Error(err)
						}
						glog.Errorf("consume cmd error:%s\n",string(jsonCmd))
					}
				}()
			}
		}

	}
}

func (self *DefalutRemotingClient) sendRequest(header, body []byte, conn net.Conn, addr string) error {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int32(len(header) + len(body) + 4))
	binary.Write(buf, binary.BigEndian, int32(len(header)))
	binary.Write(buf, binary.BigEndian, header)
	if body != nil && len(body) > 0 {
		binary.Write(buf, binary.BigEndian, body)
	}
	_, err := conn.Write(buf.Bytes())
	if err != nil {
		glog.Error(err)
		self.releaseConn(addr, conn)
		return err
	}
	return nil
}

func (self *DefalutRemotingClient) releaseConn(addr string, conn net.Conn) {
	self.connTableLock.Lock()
	delete(self.connTable, addr)
	conn.Close()
	self.connTableLock.Unlock()
}
