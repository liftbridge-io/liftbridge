.PHONY: deploy
deploy: build
	gcloud app deploy $(DEPLOY_FLAGS)

build:
	yarn --cwd .. run build
	cp -r ../build/liftbridge build

.PHONY: clean
clean:
	rm -rf build
