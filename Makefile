.PHONY: compose-up compose-down kind-up kind-down kind-apply kind-export

NAME=kind
KUBECONFIG:=~/.kube/kind-config-$(NAME)

compose-up:
	@ cd dev/; docker-compose up --build

compose-down:
	@ cd dev/; docker-compose down --rmi local

kind-up:
	kind create cluster --config k8s/dev/kind.yaml --name=$(NAME)

kind-down:
	kind delete cluster --name=$(NAME)

kind-apply:
	@ KUBECONFIG=$(KUBECONFIG) kubectl apply -R -f k8s/dev/nats-operator.yaml
	@ KUBECONFIG=$(KUBECONFIG) skaffold dev -p dev

kind-export:
	@echo export KUBECONFIG="$$(kind get kubeconfig-path --name="$(NAME)")"
