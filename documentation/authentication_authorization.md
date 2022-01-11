---
id: authentication_authorization
title: Authentication and Authorization
---

## Authentication

Liftbridge currently supports authentication via TLS

There are several parameters for TLS configuration on the server side.

```yaml

tls:
    key: server-key.pem
    cert: server-cert.pem
    client.auth.enabled: true
    client.auth.ca: ca-cert.pem
```

Refer to [Configuration TLS](./configuration.md#configuration-ettings) for details.

`client.auth.enabled` would enable client authentication, and `client.auth.ca` would specify the path on the server to the client's CA Cert.


With these configurations done on server, only authenticated clients can open connections to the server. Using `ca-cert.pem`, `client-key.pem` and `client-cert.pem`, the client can safely open a connection to a Liftbridge server.

An example Golang code to connect to a Liftbridge server using TLS:

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