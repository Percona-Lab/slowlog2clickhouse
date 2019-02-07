up:
	mkdir logs
	docker-compose up -d
	docker exec ch-server clickhouse client  --query="CREATE DATABASE IF NOT EXISTS pmm;"
	cat pmm-queries-schema.sql | docker exec -i ch-server clickhouse client -d pmm --multiline --multiquery

down:
	docker-compose down --volumes
	rm -rf logs

ch-client:
	docker exec -ti ch-server clickhouse client -d pmm

ps-client:
	docker exec -ti ps-server mysql -uroot -psecret

run: build
	./slowlog2clickhouse

build:
	go build -o slowlog2clickhouse main.go

build-linux:
	GOOS=linux go build -o slowlog2clickhouse main.go
