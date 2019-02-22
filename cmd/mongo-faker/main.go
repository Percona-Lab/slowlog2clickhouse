package main

import (
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/kshvakov/clickhouse"
)

const agentUUID = "dc889ca7be92a66f0a00f616f69ffa7b"

type mutexCounter struct {
	mu sync.Mutex
	x  int
}

func (c *mutexCounter) Add(x int) {
	c.mu.Lock()
	c.x += x
	c.mu.Unlock()
}

func (c *mutexCounter) Value() (x int) {
	c.mu.Lock()
	x = c.x
	c.mu.Unlock()
	return
}

func main() {
	logTimeStart := flag.String("logTimeStart", "2019-01-01 00:00:00", "Start fake time of query from")
	durationN := flag.Int("durationN", 60, "Generate N minutes")
	rowsPerMin := flag.Int("rowsPerMin", 100000, "Rows per minute (+/- 20%)")
	dsn := flag.String("dsn", "clickhouse://127.0.0.1:9000?database=pmm&read_timeout=10&write_timeout=200", "DSN of ClickHouse Server")
	openConns := flag.Int("open-conns", 10, "Number of open connections to ClickHouse")

	flag.Parse()
	log.SetOutput(os.Stderr)

	connect, err := sql.Open("clickhouse", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	if err = connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	connect.SetConnMaxLifetime(0)
	connect.SetMaxOpenConns(*openConns)
	connect.SetMaxIdleConns(*openConns)

	logStart, _ := time.Parse("2006-01-02 15:04:05", *logTimeStart)
	processors := runtime.GOMAXPROCS(0)
	counter := mutexCounter{}
	rowsDistrib := int(math.Round(float64(*rowsPerMin) * 0.2))
	// log10 := int(math.Round(math.Log10(float64(*rowsPerMin)))
	for counter.Value() < *durationN {
		fmt.Println("counter: ", counter.Value())
		var wg sync.WaitGroup
		for i := 0; i < processors; i++ {
			wg.Add(1)
			go func() {
				var m runtime.MemStats
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				bucketsInMinute := r.Intn(rowsDistrib) + *rowsPerMin
				counter.Add(1)
				periodStart := logStart.Add(time.Duration(counter.Value()) * time.Minute)
				fmt.Println("periodStart", periodStart)
				for i := 0; i < 10000; i++ {
					buckets := [][]interface{}{}
					for j := 0; j < bucketsInMinute/10000; j++ {
						bucket := makeValues(periodStart, r)
						buckets = append(buckets, bucket)
					}
					insertData(connect, buckets)
				}
				runtime.ReadMemStats(&m)
				fmt.Printf("Sys: %dGB \n", m.Sys/(1024*1024*1024))
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func insertData(connect *sql.DB, buckets [][]interface{}) {
	tx, err := connect.Begin()
	if err != nil {
		log.Panic(err)
	}
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		log.Panic(err)
	}

	for _, bucket := range buckets {
		_, err = stmt.Exec(bucket...)
		if err != nil {
			log.Panic(err)
		}
	}

	err = stmt.Close()
	if err != nil {
		log.Panic(err)
	}

	err = tx.Commit() // if Commit returns error update err
	if err != nil {
		fmt.Printf("cannot commit: %s", err.Error())
	}

}

func makeValues(periodStart time.Time, r *rand.Rand) []interface{} {

	labelsKey := []string{fmt.Sprintf("label%d", r.Intn(10)), fmt.Sprintf("label%d", r.Intn(10)), fmt.Sprintf("label%d", r.Intn(10))}
	labelsValue := []string{fmt.Sprintf("value%d", r.Intn(100)), fmt.Sprintf("value%d", r.Intn(100)), fmt.Sprintf("value%d", r.Intn(100))}
	numQueries := r.Intn(10000) + 1

	n := r.Intn(100)
	query := fmt.Sprintf(`{"ns":"sbtest.sbtest%d","op":"insert"}`, n)
	fingerprint := fmt.Sprintf(`INSERT sbtest%d`, n)
	queryid := fmt.Sprintf("%x", md5.Sum([]byte(fingerprint)))

	queryTime := calcStatFloat(numQueries, r)
	docsReturned := calcStat(numQueries, r)
	responseLength := calcStat(numQueries, r)
	docsScanned := calcStat(numQueries, r)
	args := []interface{}{
		queryid,                                 // queryid
		fmt.Sprintf("db%d", r.Intn(10)),         // d_server
		"",                                      // d_database
		fmt.Sprintf("schema%d", n),              // collection (postgress)
		fmt.Sprintf("user%d", r.Intn(100)),      // d_username
		fmt.Sprintf("10.11.12.%d", r.Intn(100)), // d_client_host
		labelsKey,                               // labels_key
		labelsValue,                             // labels_value
		agentUUID,                               // agent_uuid
		"mongo",                                 // metrics_source
		periodStart.Truncate(1 * time.Minute),   // period_start
		uint32(60),                              // period_length
		fingerprint,                             // fingerprint
		query,                                   // example
		0,                                       // is_truncated
		"",                                      // example_metrics
		float32(0),                              // num_queries_with_warnings
		[]string{},                              // warnings_code
		[]string{},                              // warnings_count
		float32(0),                              // num_queries_with_errors
		[]string{},                              // errors_code
		[]string{},                              // errors_count
		float32(numQueries),                     // num_queries
		queryTime.Cnt,                           // m_query_time_cnt
		queryTime.Sum,                           // m_query_time_sum
		queryTime.Min,                           // m_query_time_min
		queryTime.Max,                           // m_query_time_max
		queryTime.P99,                           // m_query_time_p99
		docsReturned.Cnt,                        // m_docs_returned_cnt
		docsReturned.Sum,                        // m_docs_returned_sum
		docsReturned.Min,                        // m_docs_returned_min
		docsReturned.Max,                        // m_docs_returned_max
		docsReturned.P99,                        // m_docs_returned_p99
		responseLength.Cnt,                      // m_response_length_cnt
		responseLength.Sum,                      // m_response_length_sum
		responseLength.Min,                      // m_response_length_min
		responseLength.Max,                      // m_response_length_max
		responseLength.P99,                      // m_response_length_p99
		docsScanned.Cnt,                         // m_docs_scanned_cnt
		docsScanned.Sum,                         // m_docs_scanned_sum
		docsScanned.Min,                         // m_docs_scanned_min
		docsScanned.Max,                         // m_docs_scanned_max
		docsScanned.P99,                         // m_docs_scanned_p99
	}
	return args
}

type stat struct {
	Cnt float32
	Sum float32
	Min float32
	Max float32
	P99 float32
}

func calcStatFloat(numQueries int, r *rand.Rand) stat {
	s := stat{}
	s.Cnt = float32(r.Intn(numQueries))
	halfMin := float32(r.Intn(30))
	t1 := float64(r.Float32() * halfMin)
	t2 := float64(r.Float32() * halfMin)
	s.Min = float32(math.Min(t1, t2))
	s.Max = float32(math.Max(t1, t2))
	s.P99 = s.Max * 0.9
	s.Sum = (s.Max + s.Min) / 2 * s.Cnt
	return s
}

func calcStat(numQueries int, r *rand.Rand) stat {
	s := stat{}
	s.Cnt = float32(r.Intn(numQueries))
	t1 := float64(r.Intn(1000))
	t2 := float64(r.Intn(1000))
	s.Min = float32(math.Min(t1, t2))
	s.Max = float32(math.Max(t1, t2))
	s.P99 = float32(math.Round(float64(s.Max * 0.9)))
	s.Sum = float32(math.Round(float64((s.Max + s.Min) / 2 * s.Cnt)))
	return s
}

const insertSQL = `
  INSERT INTO queries
  (
	queryid,
	d_server,
	d_database,
	d_schema,
	d_username,
	d_client_host,
	labels.key,
	labels.value,
	agent_uuid,
	metrics_source,
	period_start,
	period_length,
	fingerprint,
	example,
	is_truncated,
	example_metrics,
	num_queries_with_warnings,
	warnings.code,
	warnings.count,
	num_queries_with_errors,
	errors.code,
	errors.count,
	num_queries,
	m_query_time_cnt,
	m_query_time_sum,
	m_query_time_min,
	m_query_time_max,
	m_query_time_p99,
	m_docs_returned_cnt,
	m_docs_returned_sum,
	m_docs_returned_min,
	m_docs_returned_max,
	m_docs_returned_p99,
	m_response_length_cnt,
	m_response_length_sum,
	m_response_length_min,
	m_response_length_max,
	m_response_length_p99,
	m_docs_scanned_cnt,
	m_docs_scanned_sum,
	m_docs_scanned_min,
	m_docs_scanned_max,
	m_docs_scanned_p99
   ) VALUES (
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?,
	?
  )
`
