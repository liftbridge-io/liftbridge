---
id: authentication-authorization
title: Authentication and Authorization
---

Liftbridge currently supports authentication via mutual TLS. This allows both
the client to authenticate the server and the server to authenticate clients
using certificates.

Liftbridge does not currently support authorization, but ACL-based
authorization is planned for a future release.

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

Support for authorization is not yet provided. It will be implemented in the future.
