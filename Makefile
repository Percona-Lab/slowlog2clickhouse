env-up:
	mkdir logs
	docker-compose up -d
	docker exec ch-server clickhouse client  --query="CREATE DATABASE IF NOT EXISTS pmm;"
	cat pmm-queries-schema.sql | docker exec -i ch-server clickhouse client -d pmm --multiline --multiquery

env-down:
	docker-compose down --volumes
	rm -rf logs

ch-client:
	docker exec -ti ch-server clickhouse client -d pmm

ps-client:
	docker exec -ti ps-server mysql -uroot -psecret

build:
	go build -o bin/slowlogfaker cmd/slowlog-faker/main.go
	go build -o bin/slowlogloader cmd/slowlog-loader/main.go
	go build -o bin/mongofaker cmd/mongo-faker/main.go

build-linux:
	GOOS=linux go build -o bin/slowlogfaker cmd/slowlog-faker/main.go
	GOOS=linux go build -o bin/slowlogloader cmd/slowlog-loader/main.go
	GOOS=linux go build -o bin/mongofaker cmd/mongo-faker/main.go
