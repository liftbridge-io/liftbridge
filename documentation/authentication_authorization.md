---
id: authentication-authorization
title: Authentication and Authorization
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

### (Deprecated) Use a file to store ACL policies

*Storing ACL policies in simple CSV file is deprecated, as it creates many problems from administration view point. I.e: The same policy file must be guaranteed to be consistent accross all servers in the cluster*

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

*NOTE*: if `client.authz.model` or `client.authz.policy` configurations are not declared, the [default authorization behavior](./authentication_authorization.md#persist-acl-policies-directly-on-the-cluster) will be used.

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

With this method of authorization, Liftbridge does not sync policies acrosss server nodes in the cluster, so the permission is given local on the given server node. To add/remove a policy, the `policy.csv` file has to be modified manually. Liftbridge currently does not reload the file on the flight.


After a modification in `policy.csv` file, to signal Liftbridge to take into account the changes, it is required to perform one of the following actions:

- Restart the server completely ( cold reload)

- Send a `SIGHUP` signal to the server's running process to signal a reload of authorization policy (hot reload). Liftbridge handles `SIGHUP` signal to reload safely permissions without restarting.

### Persist ACL policies directly on the cluster
This is the default behavior if you have `tls.client.authz.enabled` set to `true`.


All ACL policies, once added to the cluster, will be persisted durably with Raft log. The mechanism is consistent with the way Liftbridge handles metadata information about the cluster (e.g: ISR, Stream Creation ...)

When an ACL policy is added, it will be committed to Raft log, and each participating server will make sure that the ACL policy is applied locally in the Finite State Machine (FSM), which effectively applies the policy to the server.

From administration viewpoint, as Liftbridge handles and abstracts all the complex operations behind the scence, administrator can be sure about the high level of consistency in authorization permissions on the cluster.

An example of the configuration on server side for authorization
```yaml
tls:
  key: ./configs/certs/server/server-key.pem
  cert: ./configs/certs/server/server-cert.pem
  client.auth.enabled: true
  client.auth.ca: ./configs/certs/ca-cert.pem
  client.authz.enabled: true
```

In order to add/revoke or list ACL permissions, by default, a client has to connect to the cluster as `root`. I.e: the client key and certificate must be generated for username `root`. The following requests are used to Add/Revoke or List policies:

```golang
// Create Liftbridge client.
	addrs := []string{"localhost:9292"}
	// Connect with TLS for Root user
	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile("./configs/certs/ca-cert.pem")
	if err != nil {
		panic(err)
	}
	certPool.AppendCertsFromPEM(ca)
	certificate, err := tls.LoadX509KeyPair("./configs/certs/client/client-root/client-root-cert.pem", "./configs/certs/client/client-root/client-root-key.pem")
	if err != nil {
		panic(err)
	}
	config := &tls.Config{
		ServerName:   "localhost",
		Certificates: []tls.Certificate{certificate},
		RootCAs:      certPool,
	}

	client, err := lift.Connect(addrs, lift.TLSConfig(config))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	err = client.AddPolicy(ctx, "client1", "foo", "Subscribe")
	if err != nil {
		fmt.Print("FetchMetadata")
		panic(err)
	}
	err = client.RevokePolicy(ctx, "client1", "foo", "CreateStream")
	if err != nil {
		fmt.Print("CreateStream")

		panic(err)
	}

	policies, _ := client.ListPolicy(context.Background())

	for _, policy := range policies {
		fmt.Println(policy.UserId, policy.ResourceId, policy.Action)
	}

```

The client can connect to any available server in the cluster to perform `AddPolicy`, `RevokePolicy` and `ListPolicy` requests. The request is propagated all over the cluster automatically.

A basic client needs the following permission to perform most of the functionalites on the cluster


```csv
client1, *, FetchMetadata
client1, foo, CreateStream
client1, foo, DeleteStream
client1, foo, PauseStream
client1, foo, Subscribe
client1, foo, PublishToSubject
client1, foo, Publish
client1, __cursors, Publish
client1, foo, SetStreamReadonly
client1, foo, FetchPartitionMetadata
client1, foo, SetCursor
client1, foo, FetchCursor
```

In this example, `client1` is authorized to perform a set of actions on stream `foo`.

If client needs the permissions to use `AddPolicy`, `RevokePolicy` and `ListPolicy`, the following ACL policies should be added

```
client1, *, AddPolicy
client1, *, RevokePolicy
client1, *, ListPolicy
```