APP_NAME := vilog-victorialogs
CMD := ./cmd/$(APP_NAME)

.PHONY: tidy fmt test race vet build run docker-up docker-down

tidy:
	go mod tidy

fmt:
	go fmt ./...

test:
	go test ./...

race:
	go test ./... -race

vet:
	go vet ./...

build:
	go build -o bin/$(APP_NAME) $(CMD)

run:
	go run $(CMD) -config config.example.yaml

docker-up:
	docker compose -f docker-compose.dev.yml up --build

docker-down:
	docker compose -f docker-compose.dev.yml down -v
