module fedbench

go 1.24

replace (
	golang.org/x/crypto => github.com/golang/crypto v0.36.0
	golang.org/x/exp => github.com/golang/exp v0.0.0-20250305212735-054e65f0b394
	golang.org/x/mod => github.com/golang/mod v0.24.0
	golang.org/x/net => github.com/golang/net v0.38.0
	golang.org/x/sync => github.com/golang/sync v0.12.0
	golang.org/x/sys => github.com/golang/sys v0.31.0
	golang.org/x/text => github.com/golang/text v0.23.0
	golang.org/x/tools => github.com/golang/tools v0.31.0
	google.golang.org/genproto/googleapis/rpc => github.com/googleapis/go-genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb
	google.golang.org/grpc => github.com/grpc/grpc-go v1.71.0
	google.golang.org/protobuf => github.com/protocolbuffers/protobuf-go v1.36.5
)

require (
	github.com/eclipse/paho.mqtt.golang v1.5.0
	github.com/quic-go/quic-go v0.54.0
	github.com/rabbitmq/amqp091-go v1.10.0
	google.golang.org/grpc v1.71.0
)

require (
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/quic-go/qpack v0.5.1 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/mod v0.24.0 // indirect
	golang.org/x/net v0.37.0 // indirect
	golang.org/x/sync v0.12.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	golang.org/x/tools v0.22.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)
