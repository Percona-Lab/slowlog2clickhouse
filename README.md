[![CLA assistant](https://cla-assistant.percona.com/readme/badge/Percona-Lab/slowlog2clickhouse)](https://cla-assistant.percona.com/Percona-Lab/slowlog2clickhouse)

# SlowLogToClickHouse

SlowLogToClickHouse is a tool to parse Query Classes from MySQL slow log and save it into ClickHouse table.

## Prerequisites

Required [Go](https://golang.org/doc/install) and [dep](https://github.com/golang/dep) to build on local machine.

## Build

`make build`

## Usage

```
./slowlog2clickhouse --help
Usage of ./slowlog2clickhouse:
  -dsn string
        DSN of ClickHouse Server (default "clickhouse://127.0.0.1:9000?database=pmm&x-multi-statement=true")
  -offset uint
        Start Offset of slowlog
  -slowLogPath string
        Path to MySQL slow log file (default "logs/mysql-slow.log")
```

`ex.: ./slowlog2clickhouse -dsn='clickhouse://127.0.0.1:9000?database=pmm&x-multi-statement=true' -offset=1101523 -slowLogPath=/logs/mysql-slow.log`

or `make up run` for preview

