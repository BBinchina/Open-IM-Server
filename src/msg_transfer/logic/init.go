package logic

import (
	"Open_IM/src/common/config"
	"Open_IM/src/common/kafka"
	"Open_IM/src/common/log"
)

var (
	persistentCH PersistentConsumerHandler
	historyCH    HistoryConsumerHandler
	producer     *kafka.Producer
)

func Init() {
	log.NewPrivateLog(config.Config.ModuleName.MsgTransferName)
	// 热点数据 
	persistentCH.Init()
	// 历史数据 mysql
	historyCH.Init()
	producer = kafka.NewKafkaProducer(config.Config.Kafka.Ms2pschat.Addr, config.Config.Kafka.Ms2pschat.Topic)
}
func Run() {
	//register mysqlConsumerHandler to
	go persistentCH.persistentConsumerGroup.RegisterHandleAndConsumer(&persistentCH)
	go historyCH.historyConsumerGroup.RegisterHandleAndConsumer(&historyCH)
}
