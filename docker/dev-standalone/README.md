# Standalone Docker Image

> **WARNING**: This container is intended for testing and/or development
> purposes!

Liftbridge provides lightweight, fault-tolerant message streams by implementing
a durable stream augmentation for the NATS messaging system.

This is the source code of the Docker image embedding [NATS](https://nats.io/)
next to [Liftbridge](https://github.com/liftbridge-io/liftbridge). The image is
available on Docker Hub [here](https://hub.docker.com/repository/docker/liftbridge/standalone-dev).


## Usage

Using the container can be done like this:

```
$ docker pull liftbridge/standalone-dev
$ docker run -d --name=liftbridge-main -p 4222:4222 -p 9292:9292 -p 8222:8222 -p 6222:6222 liftbridge/standalone-dev
```

This will bootup the container and start both the NATS and Liftbridge servers.
We can check the logs to see if the container booted properly:

```
$ docker logs liftbridge-main
time="2020-12-29 21:30:09" level=info msg="Liftbridge Version:        v1.5.0"
time="2020-12-29 21:30:09" level=info msg="Server ID:                 3qPpmKQXnP0J6xDOsIhsVb"
time="2020-12-29 21:30:09" level=info msg="Namespace:                 liftbridge-default"
time="2020-12-29 21:30:09" level=info msg="NATS Servers:              [nats://127.0.0.1:4222]"
time="2020-12-29 21:30:09" level=info msg="Default Retention Policy:  [Age: 1 week, Compact: false]"
time="2020-12-29 21:30:09" level=info msg="Default Partition Pausing: disabled"
time="2020-12-29 21:30:09" level=info msg="Starting embedded NATS server on 0.0.0.0:4222"
time="2020-12-29 21:30:09" level=info msg="nats: Starting nats-server version 2.1.9"
time="2020-12-29 21:30:09" level=info msg="nats: Git commit [not set]"
time="2020-12-29 21:30:09" level=info msg="nats: Starting http monitor on 0.0.0.0:8222"
time="2020-12-29 21:30:09" level=info msg="nats: Listening for client connections on 0.0.0.0:4222"
time="2020-12-29 21:30:09" level=info msg="nats: Server id is NDFWAP5HYPXXI52CKFACDHLEV2V3U4SBFDUKDPOHQ2LNIXYE2SUASBH6"
time="2020-12-29 21:30:09" level=info msg="nats: Server is ready"
time="2020-12-29 21:30:09" level=info msg="nats: Listening for route connections on 0.0.0.0:6222"
time="2020-12-29 21:30:09" level=info msg="Starting Liftbridge server on 0.0.0.0:9292..."
time="2020-12-29 21:30:10" level=info msg="Server became metadata leader, performing leader promotion actions"
```

If you want to advertise a docker host that is not localhost:

```
docker run -d --add-host registry:0.0.0.0 --name=liftbridge-main -p 4222:4222 -p 9292:9292 -p 8222:8222 -p 6222:6222 -eLIFTBRIDGE_HOST=registry liftbridge/standalone-dev
```

### Volume

Optionally you can specify the mount point with:

```--volume=/tmp/host/liftbridge:/tmp/liftbridge/liftbridge-default  ```

### Ports

Liftbridge server exposes:
- 9292 for clients.

NATS server exposes:
- 4222 for clients.
- 8222 as an HTTP management port for information reporting.
- 6222 is a routing port for clustering.

### Default NATS Configuration file

```
# Client port of 4222 on all interfaces
port: 4222

# HTTP monitoring port
monitor_port: 8222

# This is for clustering multiple servers together.
cluster {

  # Route connections to be received on any interface on port 6222
  port: 6222

  # Routes are protected, so need to use them with --routes flag
  # e.g. --routes=nats-route://ruser:T0pS3cr3t@otherdockerhost:6222
  authorization {
    user: ruser
    password: T0pS3cr3t
    timeout: 0.75
  }

  # Routes are actively solicited and connected to from this server.
  # This Docker image has none by default, but you can pass a
  # flag to the nats-server docker image to create one to an existing server.
  routes = []
}
```

## Development

### Building

Go to the root directory of the Liftbridge source code and run:

```
$ docker build -t liftbridge/standalone-dev -f docker/dev-standalone/Dockerfile .
```
