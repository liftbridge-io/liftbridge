---
id: version-v1.9.0-authentication-authorization
title: Authentication and Authorization
original_id: authentication-authorization
---

Liftbridge currently supports authentication via mutual TLS. This allows both
the client to authenticate the server and the server to authenticate clients
using certificates.

Liftbridge currently provides a simple ACL-based authorization mechanism. This feature is provided in experimental mode. Further improvements will be rolled out in future releases.

## Authentication

Authentication is currently supported using mutual TLS. There are several
parameters for TLS configuration on the server side.

```yaml
tls:
    key: server-key.pem
    cert: server-cert.pem
    client.auth.enabled: true
    client.auth.ca: ca-cert.pem
```

`client.auth.enabled` enables client authentication, and `client.auth.ca`
specifies the path on the server to the client's certificate authority. Refer
to the `tls` settings in
[Configuration](./configuration.md#configuration-settings) for more details.

With these configurations set on the server, only authenticated clients can
open connections to the server. Using `ca-cert.pem`, `client-key.pem` and
`client-cert.pem`, the client can safely open a connection to a Liftbridge
server. Example Go code to connect to a Liftbridge server using TLS is shown
below:

```golang
certPool := x509.NewCertPool()
ca, err := ioutil.ReadFile("ca-cert.pem")
if err != nil {
	panic(err)
}
certPool.AppendCertsFromPEM(ca)
certificate, err := tls.LoadX509KeyPair("client-cert.pem", "client-key.pem")
if err != nil {
	panic(err)
}
config := &tls.Config{
	ServerName:   "localhost",
	Certificates: []tls.Certificate{certificate},
	RootCAs:      certPool,
}
client, err := lift.Connect([]string{"localhost:9292"}, lift.TLSConfig(config))
```

## Authorization

*This feature is experimental. Further improvements will be provided in later releases.*

A simple ACL-based authorization mechanism is supported. It is provided thanks to [Casbin ACL models](https://github.com/casbin/casbin#examples).

Liftbridge identifies clients thanks to TLS certificates. Thus, in order to use ACL based authorization, TLS configurations for authentication must be enabled first.

See the [above section](#authentication) to properly enable authentication.

In order to define permissions for clients on specific resources, a `model.conf` file and a `policy.csv` file is required. The `model.conf` file will define the ACL based authorization models. The `policy.csv` serves as a local file-based storage for authorization policies. Currently polices are not yet synchronized or persisted automatically acrosss Liftbridge cluster. Support for this may be provided in the future.

Refer to the `tls` settings in
[Configuration](./configuration.md#configuration-settings) for more details.

An example of configuration on server side for authorization

```yaml
tls:
  key: ./configs/certs/server/server-key.pem
  cert: ./configs/certs/server/server-cert.pem
  client.auth.enabled: true
  client.auth.ca: ./configs/certs/ca-cert.pem
  client.authz.enabled: true
  client.authz.model: ./configs/authz/model.conf
  client.authz.policy: ./configs/authz/policy.csv

```

Inside `model.conf`, an ACL based models should be defined, as

```conf
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.sub == p.sub && r.obj == p.obj && r.act == p.act

```

And `policy.csv` should declare authorization policies to be applied

```csv
p, client1, *, FetchMetadata
p, client1, foo, CreateStream
p, client1, foo, DeleteStream
p, client1, foo, PauseStream
p, client1, foo, Subscribe
p, client1, foo, PublishToSubject
p, client1, foo, Publish
p, client1, __cursors, Publish
p, client1, foo, SetStreamReadonly
p, client1, foo, FetchPartitionMetadata
p, client1, foo, SetCursor
p, client1, foo, FetchCursor
```

Refer to [Casbin ACL models](https://github.com/casbin/casbin#examples) for details on ACL model and policy.

In this example, `client1` is authorized to perform a set of actions on stream `foo`.

**NOTE**: 
- In order to connect to a server, the client will systematically call `FetchMetadata` to fetch servers's connection metadata. Thus, a client must always have `FetchMetadata` permission on resource `*`. 
- In order to use `cursor`, the client must also have permissions on stream `__cursors`.
- `policy.csv` is the local file to store authorization policy. A corrupted file may result in API fails to server requests (due to policy configuration errors), or API crashes ( if the `policy.csv` is totally corrupted).

As mentioned, currently Liftbridge does not sync policies acrosss server nodes in the cluster, so the permission is given local on the given server node. To add/remove a policy, the `policy.csv` file has to be modified manually. Liftbridge currently does not reload the file on the flight.

### Permission reload

After a modification in `policy.csv` file, to signal Liftbridge to take into account the changes, it is required to perform one of the following actions:

- Restart the server completely ( cold reload)

- Send a `SIGHUP` signal to the server's running process to signal a reload of authorization policy (hot reload). Liftbridge handles `SIGHUP` signal to reload safely permissions without restarting.