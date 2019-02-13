[![CLA assistant](https://cla-assistant.percona.com/readme/badge/Percona-Lab/slowlog2clickhouse)](https://cla-assistant.percona.com/Percona-Lab/slowlog2clickhouse)

# SlowLogToClickHouse

Contains two tools:
1. tool to load slowlog into clickhouse.
2. tool to load from slowlog modified/faked data such as timestamp, dimantions, labels etc, and metrics into clickhouse multiply times.
SlowLogToClickHouse is a tool to parse Query Classes from MySQL slow log and save it into ClickHouse table.

## Prerequisites

Required [Go](https://golang.org/doc/install) and [dep](https://github.com/golang/dep) to build on local machine.

## Build

`make build`

## Usage

```
./bin/slowlogloader --help
Usage of ./bin/slowlogloader:
  -dsn string
        DSN of ClickHouse Server (default "clickhouse://127.0.0.1:9000?database=pmm")
  -slowLogPath string
        Path to MySQL slow log file (default "logs/mysql-slow.log")
```

`ex.: ./slowlogloader -dsn='clickhouse://127.0.0.1:9000?database=sbtest' -slowLogPath=/logs/mysql-slow.log`

```
./bin/slowlogfaker --help
Usage of ./bin/slowlogfaker:
  -dsn string
        DSN of ClickHouse Server (default "clickhouse://127.0.0.1:9000?database=pmm")
  -logTimeStart string
        Start fake time of query from (default "2019-01-01 00:00:00")
  -repeatN int
        Scan slowlog given times (when 0 will wait for new evens)
  -slowLogPath string
        Path to MySQL slow log file (default "logs/mysql-slow.log")
```

`ex.: ./slowlogfaker -dsn='clickhouse://127.0.0.1:9000?database=1_day_db' -repeatN=24 -logTimeStart="2019-02-10 00:00:00" -slowLogPath=/logs/1hour-slow-1hour.log`
