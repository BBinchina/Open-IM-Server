package main

import (
	"Open_IM/src/msg_transfer/logic"
	"sync"
)
// 启动kafka的消费者，用于将消息持久化，分别写入monogo 跟 mysql
func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	logic.Init()
	logic.Run()
	wg.Wait()
}
