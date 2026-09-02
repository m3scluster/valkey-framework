module valkey-mesos-framework

go 1.26

require (
	github.com/m3scluster/clusterd-go v0.0.0
	github.com/redis/go-redis/v9 v9.22.0
	github.com/sirupsen/logrus v1.9.4
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/m3scluster/clusterd-go => /home/andreas/Projekte/go/clusterd-go
