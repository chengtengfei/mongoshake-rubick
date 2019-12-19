package conf

type Configuration struct {
	Tunnel        					string `config:"tunnel"`
	TunnelAddress 					string `config:"tunnel.address"`
	SystemProfile 					int    `config:"system_profile"`
	LogDirectory  					string `config:"log.dir"`
	LogLevel      					string `config:"log.level"`
	LogFileName   					string `config:"log.file"`
	LogBuffer     					bool   `config:"log.buffer"`
	ReplayerNum   					int    `config:"replayer"`
	DestMongoAddress  				string `config:"dest.address"`
	DestMongoDB 					string `config:"dest.db"`
	DestMongoUsername 				string `config:"dest.username"`
	DestMongoPassword				string `config:"dest.password"`
	KafkaConsumerOffsetFile 		string `config:"kafka.consumer.offset.file"`
	KafkaConsumerOffset				int64
}

var Options Configuration
