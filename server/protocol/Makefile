build:
	protoc -I. \
		--gofast_out=\
Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,\
plugins=grpc:. \
		*.proto