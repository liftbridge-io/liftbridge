.PHONY: deploy
deploy: build
	gcloud app deploy

build:
	yarn --cwd .. run build
	cp -r ../build/liftbridge build

.PHONY: clean
clean:
	rm -rf build
