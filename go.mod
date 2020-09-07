module github.com/liftbridge-io/liftbridge

go 1.14

require (
	github.com/Workiva/go-datastructures v1.0.52
	github.com/armon/go-metrics v0.3.4 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/golang/protobuf v1.4.2
	github.com/hako/durafmt v0.0.0-20200605151348-3a43fc422dd9
	github.com/hashicorp/go-hclog v0.14.1 // indirect
	github.com/hashicorp/go-immutable-radix v1.2.0 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/raft v1.1.2
	github.com/liftbridge-io/go-liftbridge v1.0.1-0.20200901163447-38f14d24c90d
	github.com/liftbridge-io/liftbridge-api v1.1.1-0.20200902210540-e8e022c522b7
	github.com/liftbridge-io/nats-on-a-log v0.0.0-20200818183806-bb17516cf3a3
	github.com/liftbridge-io/raft-boltdb v0.0.0-20200414234651-aaf6e08d8f73
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mitchellh/mapstructure v1.3.2 // indirect
	github.com/natefinch/atomic v0.0.0-20200526193002-18c0533a5b09
	github.com/nats-io/jwt v1.0.1 // indirect
	github.com/nats-io/nats-server/v2 v2.1.4
	github.com/nats-io/nats.go v1.10.0
	github.com/nats-io/nkeys v0.2.0 // indirect
	github.com/nats-io/nuid v1.0.1
	github.com/nsip/gommap v0.0.0-20181229045655-f7881c3a959f
	github.com/pelletier/go-toml v1.8.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/afero v1.3.1 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli v1.22.4
	go.etcd.io/bbolt v1.3.5 // indirect
	golang.org/x/crypto v0.0.0-20200728195943-123391ffb6de // indirect
	google.golang.org/grpc v1.31.1
	gopkg.in/ini.v1 v1.57.0 // indirect
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
)

// Temporary replacements until upstream merge.
replace github.com/liftbridge-io/go-liftbridge => github.com/ably-forks/go-liftbridge v1.0.1-0.20200907110023-38daabe19873

replace github.com/liftbridge-io/liftbridge-api => github.com/ably-forks/liftbridge-api v1.9.3
