.PHONY: dev

GITREF = `git rev-list HEAD |head -n 1`
CI_COMMIT_REF_SLUG = `git rev-parse --abbrev-ref HEAD`

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

dev:
	@ cd dev/; docker-compose up --build

build-dev-image:
	@ docker build -t "liftbridge:dev" -f dev/Dockerfile.compose .

build-image:
	@ docker build -t "liftbridge/liftbridge:$(CI_COMMIT_REF_SLUG)" -t "liftbridge/liftbridge:$(GITREF)" -f Dockerfile .
