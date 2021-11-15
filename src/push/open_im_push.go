package main

import (
	"Open_IM/src/push/logic"
	"flag"
	"sync"
)
// 平台消息推送服务， 用于推送消息给消息网关，再通知到用户
func main() {
	rpcPort := flag.Int("port", -1, "rpc listening port")
	flag.Parse()
	var wg sync.WaitGroup
	wg.Add(1)
	logic.Init(*rpcPort)
	logic.Run()
	wg.Wait()
}
