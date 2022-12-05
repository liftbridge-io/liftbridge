package server

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc "google.golang.org/grpc"
	credentials "google.golang.org/grpc/credentials"
	peer "google.golang.org/grpc/peer"
)

// Default ACL with superuser model for Casbin authorization.
// Ref: https://github.com/casbin/casbin/blob/master/examples/basic_with_root_model.conf
var DefaultACLAuthzModel string = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.sub == p.sub && r.obj == p.obj && r.act == p.act || r.sub == "root"
`

// addUserContext parses client ID from context and set client ID in context
func addUserContext(ctx context.Context) context.Context {
	p, ok := peer.FromContext(ctx)

	if !ok || p.AuthInfo == nil {
		return ctx
	}

	tlsInfo := p.AuthInfo.(credentials.TLSInfo)

	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return ctx
	}

	clientName := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	return context.WithValue(ctx, "clientID", clientName)

}

// AuthzUnaryInterceptor gets user from TLS-authenticated request and add user to ctx
func AuthzUnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	return handler(addUserContext(ctx), req)
}

// AuthzStreamInterceptor gets user from TLS-authenticated stream request and add user to ctx
func AuthzStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	newStream := grpc_middleware.WrapServerStream(ss)
	newStream.WrappedContext = addUserContext(ss.Context())
	return handler(srv, newStream)
}
