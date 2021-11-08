VERSION:=0.2.0
build:
	@ docker build -t liftbridge/liftbridge-circleci:$(VERSION) -f Dockerfile ../../
publish:
	@ docker push liftbridge/liftbridge-circleci:$(VERSION)
