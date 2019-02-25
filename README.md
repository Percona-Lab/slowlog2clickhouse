# SlowLogToClickHouse

[![CLA assistant](https://cla-assistant.percona.com/readme/badge/Percona-Lab/slowlog2clickhouse)](https://cla-assistant.percona.com/Percona-Lab/slowlog2clickhouse)

SlowLogToClickHouse is a tool to parse/fake Query Classes from MySQL slow log (Mongo) and save it into ClickHouse table.

Contains tools:

1. tool to load slowlog into clickhouse.
2. tool to load from slowlog modified/faked data such as timestamp, dimentions, labels etc, and metrics into clickhouse multiply times.
3. tool generate MongoDB fake data such as timestamp, dimentions, labels etc, and metrics into clickhouse multiply times.

## Prerequisites

Required [Go](https://golang.org/doc/install) and [dep](https://github.com/golang/dep) to build on local machine.

## Build

`make build`

## Usage

```raw
./bin/slowlogloader --help
Usage of ./bin/slowlogloader:
  -dsn string
        DSN of ClickHouse Server (default "clickhouse://127.0.0.1:9000?database=pmm")
  -slowLogPath string
        Path to MySQL slow log file (default "logs/mysql-slow.log")
```

`ex.: ./slowlogloader -dsn='clickhouse://127.0.0.1:9000?database=sbtest' -slowLogPath=/logs/mysql-slow.log`

```raw
./bin/slowlogfaker --help
Usage of ./bin/slowlogfaker:
  -dsn string
        DSN of ClickHouse Server (default "clickhouse://127.0.0.1:9000?database=pmm")
  -logTimeStart string
        Start fake time of query from (default "2019-01-01 00:00:00")
  -max-rand-num-queries int
        Maximum of random num_queries, if 0 - num_queries from slow log (default 10000)
  -open-conns int
        Number of open connections to ClickHouse (default 10)
  -repeatN int
        Scan slowlog given times (when 0 will wait for new evens)
  -slowLogPath string
        Path to MySQL slow log file (default "logs/mysql-slow.log")
```

`ex.: ./slowlogfaker -dsn='clickhouse://127.0.0.1:9000?database=1_day_db' -repeatN=24 -logTimeStart="2019-02-10 00:00:00" -slowLogPath=/logs/1hour-slow-1hour.log -max-rand-num-queries=0`

```raw
./bin/mongofaker --help
Usage of ./bin/mongofaker:
  -dsn string
        DSN of ClickHouse Server (default "clickhouse://127.0.0.1:9000?database=pmm&read_timeout=10&write_timeout=200")
  -durationN int
        Generate N minutes (default 60)
  -logTimeStart string
        Start fake time of query from (default "2019-01-01 00:00:00")
  -open-conns int
        Number of open connections to ClickHouse (default 10)
  -rowsPerMin int
        Rows per minute (+/- 20%) (default 10000)
```

`ex.: ./slowlogfaker -dsn='clickhouse://127.0.0.1:9000?database=1_day_db' -repeatN=24 -logTimeStart="2019-02-10 00:00:00" -slowLogPath=/logs/1hour-slow-1hour.log -max-rand-num-queries=0`
