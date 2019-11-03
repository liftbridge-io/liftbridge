.PHONY: compose-up compose-down kind-up kind-down kind-apply kind-export

KIND_CLUSTER_NAME=kind
KIND_KUBECONFIG:=~/.kube/kind-config-$(KIND_CLUSTER_NAME)

compose-up:
	@ cd dev/; docker-compose up --build

compose-down:
	@ cd dev/; docker-compose down --rmi local

kind-up:
	kind create cluster --config k8s/dev/kind.yaml --name=$(KIND_CLUSTER_NAME)

kind-down:
	kind delete cluster --name=$(KIND_CLUSTER_NAME)

kind-apply:
	@ KUBECONFIG=$(KIND_KUBECONFIG) kubectl apply -R -f k8s/dev/nats-operator.yaml
	@ KUBECONFIG=$(KIND_KUBECONFIG) skaffold dev -p dev

kind-export:
	@echo export KUBECONFIG="$$(kind get kubeconfig-path --name="$(KIND_CLUSTER_NAME)")"
