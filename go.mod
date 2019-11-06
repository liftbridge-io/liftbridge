module github.com/liftbridge-io/liftbridge

go 1.13

require (
	github.com/Workiva/go-datastructures v1.0.50
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/golang/protobuf v1.3.2
	github.com/hako/durafmt v0.0.0-20191009132224-3f39dc1ed9f4
	github.com/hashicorp/raft v1.1.1
	github.com/hashicorp/raft-boltdb v0.0.0-20191021154308-4207f1bf0617
	github.com/kr/pretty v0.1.0 // indirect
	github.com/liftbridge-io/go-liftbridge v0.0.0-20191106171334-84163faf9fdd
	github.com/liftbridge-io/liftbridge-api v0.0.0-20190910222614-5694b15f251d
	github.com/liftbridge-io/nats-on-a-log v0.0.0-20190703144237-760cefbfc85e
	github.com/natefinch/atomic v0.0.0-20150920032501-a62ce929ffcc
	github.com/nats-io/go-nats v1.7.2 // indirect
	github.com/nats-io/nats-server/v2 v2.1.0
	github.com/nats-io/nats.go v1.9.1
	github.com/nats-io/nuid v1.0.1
	github.com/nsip/gommap v0.0.0-20181229045655-f7881c3a959f
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli v1.22.1
	google.golang.org/grpc v1.25.0
)

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20190409202823-959b441ac422
