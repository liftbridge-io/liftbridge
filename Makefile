.PHONY: dev

GITREF = `git rev-list HEAD |head -n 1`
CI_COMMIT_REF_SLUG = `git rev-parse --abbrev-ref HEAD`
NAME=kind
KUBECONFIG:=~/.kube/kind-config-$(NAME)

dev:
	@ cd dev/; docker-compose up --build

build-dev-image:
	@ docker build -t "liftbridge:dev" -f dev/Dockerfile.compose .

build-image:
	@ docker build -t "liftbridge/liftbridge:$(CI_COMMIT_REF_SLUG)" -t "liftbridge/liftbridge:$(GITREF)" -f Dockerfile .

kind-up:
	kind create cluster --config k8s/dev/kind.yaml --name=$(NAME)

kind-down:
	kind delete cluster --name=$(NAME)

kind-apply:
	@ KUBECONFIG=$(KUBECONFIG) kubectl apply -R -f k8s/dev/nats-operator.yaml
	@ KUBECONFIG=$(KUBECONFIG) skaffold dev -p dev

kind-export:
	@echo export KUBECONFIG="$$(kind get kubeconfig-path --name="$(NAME)")"
