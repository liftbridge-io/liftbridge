KIND_CLUSTER_NAME=kind
KIND_KUBECONFIG:=~/.kube/kind-config-$(KIND_CLUSTER_NAME)

.PHONY: compose-up
compose-up:
	cd docker/dev-cluster/; docker-compose up --build

.PHONY: compose-down
compose-down:
	cd docker/dev-cluster/; docker-compose down --rmi local

.PHONY: push-k8s-image
push-k8s-image:
	skaffold run -p deploy-k8s-image

.PHONY: kind-up
kind-up:
	kind create cluster --config k8s/dev/kind.yaml --name=$(KIND_CLUSTER_NAME)

.PHONY: kind-down
kind-down:
	kind delete cluster --name=$(KIND_CLUSTER_NAME)

.PHONY: kind-apply
kind-apply:
	KUBECONFIG=$(KIND_KUBECONFIG) kubectl apply -R -f k8s/dev/nats-operator.yaml
	KUBECONFIG=$(KIND_KUBECONFIG) skaffold dev -p dev

.PHONY: kind-export
kind-export:
	echo export KUBECONFIG="$$(kind get kubeconfig-path --name="$(KIND_CLUSTER_NAME)")"

build: liftbridge
liftbridge:
	GO111MODULE=on CGO_ENABLED=0 go build -mod=readonly -o liftbridge

build-dev: liftbridge-dev
liftbridge-dev:
	CGO_ENABLED=1 go build -tags netgo -ldflags '-extldflags "-static"' -mod=readonly -o liftbridge-dev

.PHONY: clean
clean:
	rm -f liftbridge
	rm -f liftbridge-dev

.PHONY: website-deploy
website-deploy: website/build
	gcloud app deploy $(WEBSITE_DEPLOY_FLAGS) website/app.yaml

website/build:
	yarn --cwd website run build

.PHONY: website-clean
website-clean:
	rm -rf website/build

.PHONY: website-up
website-up:
	yarn --cwd website start


