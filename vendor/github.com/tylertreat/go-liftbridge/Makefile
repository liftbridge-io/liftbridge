PROTO_REPO = liftbridge-grpc

install: $(PROTO_REPO)
	go generate

$(PROTO_REPO):
	git clone git@github.com:tylertreat/$(PROTO_REPO).git

clean:
	rm -rf $(PROTO_REPO)
