package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/kshvakov/clickhouse"
	"github.com/percona/go-mysql/event"
	slowlog "github.com/percona/go-mysql/log"
	parser "github.com/percona/go-mysql/log/slow"
	"github.com/percona/go-mysql/query"
)

var opt = slowlog.Options{}

const agentUUID = "dc889ca7be92a66f0a00f616f69ffa7b"

func main() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// zipf9 := rand.NewZipf(r, 5, 4, 9)
	// zipf99 := rand.NewZipf(r, 5, 42, 99)

	slowLogPath := flag.String("slowLogPath", "logs/mysql-slow.log", "Path to MySQL slow log file")
	logTimeStart := flag.String("logTimeStart", "2019-01-01 00:00:00", "Start fake time of query from")
	repeatN := flag.Int("repeatN", 0, "Scan slowlog given times (when 0 will wait for new evens)")
	dsn := flag.String("dsn", "clickhouse://127.0.0.1:9000?database=pmm", "DSN of ClickHouse Server")

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

	events := parseSlowLog(*slowLogPath, opt)
	fmt.Println("Parsing slowlog: ", *slowLogPath, "...")
	logStart, _ := time.Parse("2006-01-02 15:04:05", *logTimeStart)
	periodNumber := 0
	iteration := 1
	fmt.Printf("Iteration %d of %d \n", iteration, *repeatN)
	for {
		i := 0
		aggregator := event.NewAggregator(true, 0, 0)
		prewTs := time.Time{}
		periodStart := time.Time{}

		for e := range events {
			fingerprint := query.Fingerprint(e.Query)
			digest := query.Id(fingerprint)
			// duration := time.Duration(iteration*(*incrementBySeconds) + *secondsOffset)
			// e.Ts = e.Ts.Add(duration * time.Hour)

			// e.Db = fmt.Sprintf("schema%v", zipf99.Uint64()+1)      // fake data
			// e.User = fmt.Sprintf("user%v", zipf99.Uint64()+1)      // fake data
			// e.Server = fmt.Sprintf("db%v", zipf9.Uint64()+1)       // fake data
			// e.Host = fmt.Sprintf("10.11.12.%v", zipf99.Uint64()+1) // fake data

			e.Db = fmt.Sprintf("schema%d", r.Intn(100))      // fake data 100
			e.User = fmt.Sprintf("user%d", r.Intn(100))      // fake data 100
			e.Host = fmt.Sprintf("10.11.12.%d", r.Intn(100)) // fake data 100
			e.Server = fmt.Sprintf("db%d", r.Intn(10))       // fake data 10
			e.LabelsKey = []string{fmt.Sprintf("label%d", r.Intn(10)), fmt.Sprintf("label%d", r.Intn(10)), fmt.Sprintf("label%d", r.Intn(10))}
			e.LabelsValue = []string{fmt.Sprintf("value%d", r.Intn(100)), fmt.Sprintf("value%d", r.Intn(100)), fmt.Sprintf("value%d", r.Intn(100))}

			aggregator.AddEvent(e, digest, e.User, e.Host, e.Db, e.Server, fingerprint)

			// Pass last offset to restart reader when reached out end of slowlog.
			opt.StartOffset = e.OffsetEnd

			i++

			if periodStart.IsZero() {
				periodStart = logStart.Add(time.Duration(periodNumber) * time.Minute)
			}

			if prewTs.IsZero() {
				prewTs = e.Ts
			}

			if e.Ts.Sub(prewTs).Seconds() > 59 {
				prewTs = e.Ts
				break
			}
		}
		periodNumber++

		// No new events in slowlog. Nothing to save in ClickHouse. New iteration.
		if i == 0 {
			iteration++
			fmt.Printf("Iteration %d of %d \n", iteration, *repeatN)
			if iteration > *repeatN {
				fmt.Printf("Done. Total Iterations %v \n", iteration)
				break
			}
			opt.StartOffset = 0
			events = parseSlowLog(*slowLogPath, opt)
			continue
		}

		res := aggregator.Finalize()

		j := 0
		classesLen := len(res.Class) - 1
		var stmt *sql.Stmt
		var tx *sql.Tx
		for _, v := range res.Class {

			// If key has suffix _time or _wait than field is TimeMetrics.
			// For Boolean metrics exists only Sum.
			// https://www.percona.com/doc/percona-server/5.7/diagnostics/slow_extended.html
			// TimeMetrics: query_time, lock_time, rows_sent, innodb_io_r_wait, innodb_rec_lock_wait, innodb_queue_wait.
			// NumberMetrics: rows_examined, rows_affected, rows_read, merge_passes, innodb_io_r_ops, innodb_io_r_bytes,
			// innodb_pages_distinct, query_length, bytes_sent, tmp_tables, tmp_disk_tables, tmp_table_sizes.
			// BooleanMetrics: qc_hit, full_scan, full_join, tmp_table, tmp_table_on_disk, filesort, filesort_on_disk,
			// select_full_range_join, select_range, select_range_check, sort_range, sort_rows, sort_scan,
			// no_index_used, no_good_index_used.

			if j%100000 == 0 {
				tx, err = connect.Begin()
				if err != nil {
					fmt.Printf("transaction begin error: %v", err)
				}
				stmt, err = tx.Prepare(insertSQL)
				if err != nil {
					fmt.Printf("prepare error: %v", err)
				}
			}
			numQueries := float32(r.Intn(10000) + 1)
			args := makeValues(v, periodStart, numQueries)
			_, err = stmt.Exec(args...)
			if err != nil {
				fmt.Printf("exec error: %v", err)
			}

			if j >= classesLen || j%100000 == 99999 {
				err = stmt.Close()
				if err != nil {
					fmt.Printf("cannot close: %s", err.Error())
				}

				err = tx.Commit() // if Commit returns error update err
				if err != nil {
					fmt.Printf("cannot commit: %s", err.Error())
				}
			}
			j++
		}
		fmt.Printf("%d/%d queries / query classes \n", i, j)
	}
}

// If key has suffix _time or _wait than field is TimeMetrics.
// For Boolean metrics exists only Sum.
// https://www.percona.com/doc/percona-server/5.7/diagnostics/slow_extended.html
// TimeMetrics: query_time, lock_time, rows_sent, innodb_io_r_wait, innodb_rec_lock_wait, innodb_queue_wait.
// NumberMetrics: rows_examined, rows_affected, rows_read, merge_passes, innodb_io_r_ops, innodb_io_r_bytes,
// innodb_pages_distinct, query_length, bytes_sent, tmp_tables, tmp_disk_tables, tmp_table_sizes.
// BooleanMetrics: qc_hit, full_scan, full_join, tmp_table, tmp_table_on_disk, filesort, filesort_on_disk,
// select_full_range_join, select_range, select_range_check, sort_range, sort_rows, sort_scan,
// no_index_used, no_good_index_used.
func makeValues(v *event.Class, periodStart time.Time, numQueries float32) []interface{} {
	// t, _ := time.Parse("2006-01-02 15:04:05", v.Example.Ts)
	args := []interface{}{
		v.Id,                                  // digest
		v.Fingerprint,                         // digest_text
		v.Server,                              // db_server
		v.Db,                                  // db_schema
		v.User,                                // db_username
		v.Host,                                // client_host
		v.LabelsKey,                           // labels_key
		v.LabelsValue,                         // labels_value
		agentUUID,                             // agent_uuid
		periodStart.Truncate(1 * time.Minute), // period_start
		float32(60),                           // period_length
		v.Example.Query,                       // example
		0,                                     // is_truncated
		"",                                    // example_metrics
		float32(0),                            // num_queries_with_warnings
		[]string{},                            // warnings_code
		[]string{},                            // warnings_count
		float32(0),                            // num_query_with_errors
		[]string{},                            // errors_code
		[]string{},                            // errors_count
		numQueries,                            // num_queries
	}

	metricNames := []string{
		"Query_time",
		"Lock_time",
		"Rows_sent",
		"Rows_examined",
		"Rows_affected",
		"Rows_read",
		"Merge_passes",
		"InnoDB_IO_r_ops",
		"InnoDB_IO_r_bytes",
		"InnoDB_IO_r_wait",
		"InnoDB_rec_lock_wait",
		"InnoDB_queue_wait",
		"InnoDB_pages_distinct",
		"Query_length",
		"Bytes_sent",
		"Tmp_tables",
		"Tmp_disk_tables",
		"Tmp_table_sizes",
	}

	for _, mName := range metricNames {
		a := []interface{}{float32(0), float32(0), float32(0), float32(0), float32(0), []float32{}}
		if m, ok := v.Metrics.NumberMetrics[mName]; ok {
			a = []interface{}{float32(0), float32(m.Sum), float32(*m.Min), float32(*m.Max), float32(*m.P95), []float32{}}
		}
		// in case of "_wait" suffix
		if m, ok := v.Metrics.TimeMetrics[mName]; ok {
			a = []interface{}{float32(0), float32(m.Sum), float32(*m.Min), float32(*m.Max), float32(*m.P95), []float32{}}
		}
		args = append(args, a...)
	}

	boolMetricNames := []string{
		"QC_Hit",
		"Full_scan",
		"Full_join",
		"Tmp_table",
		"Tmp_table_on_disk",
		"Filesort",
		"Filesort_on_disk",
		"Select_full_range_join",
		"Select_range",
		"Select_range_check",
		"Sort_range",
		"Sort_rows",
		"Sort_scan",
		"No_index_used",
		"No_good_index_used",
	}
	for _, mName := range boolMetricNames {
		sum := float32(0)
		if m, ok := v.Metrics.BoolMetrics[mName]; ok {
			sum = float32(m.Sum)
		}
		args = append(args, sum)
	}

	// grpstr, grpint, labint_key, labint_value
	a := []interface{}{"", float32(0), []float32{}, []float32{}}
	args = append(args, a...)
	return args
}

func parseSlowLog(filename string, o slowlog.Options) <-chan *slowlog.Event {
	file, err := os.Open(filepath.Clean(filename))
	if err != nil {
		log.Fatal("cannot open slowlog", err)
	}
	p := parser.NewSlowLogParser(file, o)
	go func() {
		err = p.Start()
		if err != nil {
			log.Fatal("cannot start parser", err)
		}
	}()
	return p.EventChan()
}

const insertSQL = `
  INSERT INTO queries
  (
	digest,
	digest_text,
	db_server,
	db_schema,
	db_username,
	client_host,
	labels.key,
	labels.value,
	agent_uuid,
	period_start,
	period_length,
	example,
	is_truncated,
	example_metrics,
	num_queries_with_warnings,
	warnings.code,
	warnings.count,
	num_query_with_errors,
	errors.code,
	errors.count,
	num_queries,
	m_query_time_cnt,
	m_query_time_sum,
	m_query_time_min,
	m_query_time_max,
	m_query_time_p99,
	m_query_time_hg,
	m_lock_time_cnt,
	m_lock_time_sum,
	m_lock_time_min,
	m_lock_time_max,
	m_lock_time_p99,
	m_lock_time_hg,
	m_rows_sent_cnt,
	m_rows_sent_sum,
	m_rows_sent_min,
	m_rows_sent_max,
	m_rows_sent_p99,
	m_rows_sent_hg,
	m_rows_examined_cnt,
	m_rows_examined_sum,
	m_rows_examined_min,
	m_rows_examined_max,
	m_rows_examined_p99,
	m_rows_examined_hg,
	m_rows_affected_cnt,
	m_rows_affected_sum,
	m_rows_affected_min,
	m_rows_affected_max,
	m_rows_affected_p99,
	m_rows_affected_hg,
	m_rows_read_cnt,
	m_rows_read_sum,
	m_rows_read_min,
	m_rows_read_max,
	m_rows_read_p99,
	m_rows_read_hg,
	m_merge_passes_cnt,
	m_merge_passes_sum,
	m_merge_passes_min,
	m_merge_passes_max,
	m_merge_passes_p99,
	m_merge_passes_hg,
	m_innodb_io_r_ops_cnt,
	m_innodb_io_r_ops_sum,
	m_innodb_io_r_ops_min,
	m_innodb_io_r_ops_max,
	m_innodb_io_r_ops_p99,
	m_innodb_io_r_ops_hg,
	m_innodb_io_r_bytes_cnt,
	m_innodb_io_r_bytes_sum,
	m_innodb_io_r_bytes_min,
	m_innodb_io_r_bytes_max,
	m_innodb_io_r_bytes_p99,
	m_innodb_io_r_bytes_hg,
	m_innodb_io_r_wait_cnt,
	m_innodb_io_r_wait_sum,
	m_innodb_io_r_wait_min,
	m_innodb_io_r_wait_max,
	m_innodb_io_r_wait_p99,
	m_innodb_io_r_wait_hg,
	m_innodb_rec_lock_wait_cnt,
	m_innodb_rec_lock_wait_sum,
	m_innodb_rec_lock_wait_min,
	m_innodb_rec_lock_wait_max,
	m_innodb_rec_lock_wait_p99,
	m_innodb_rec_lock_wait_hg,
	m_innodb_queue_wait_cnt,
	m_innodb_queue_wait_sum,
	m_innodb_queue_wait_min,
	m_innodb_queue_wait_max,
	m_innodb_queue_wait_p99,
	m_innodb_queue_wait_hg,
	m_innodb_pages_distinct_cnt,
	m_innodb_pages_distinct_sum,
	m_innodb_pages_distinct_min,
	m_innodb_pages_distinct_max,
	m_innodb_pages_distinct_p99,
	m_innodb_pages_distinct_hg,
	m_query_length_cnt,
	m_query_length_sum,
	m_query_length_min,
	m_query_length_max,
	m_query_length_p99,
	m_query_length_hg,
	m_bytes_sent_cnt,
	m_bytes_sent_sum,
	m_bytes_sent_min,
	m_bytes_sent_max,
	m_bytes_sent_p99,
	m_bytes_sent_hg,
	m_tmp_tables_cnt,
	m_tmp_tables_sum,
	m_tmp_tables_min,
	m_tmp_tables_max,
	m_tmp_tables_p99,
	m_tmp_tables_hg,
	m_tmp_disk_tables_cnt,
	m_tmp_disk_tables_sum,
	m_tmp_disk_tables_min,
	m_tmp_disk_tables_max,
	m_tmp_disk_tables_p99,
	m_tmp_disk_tables_hg,
	m_tmp_table_sizes_cnt,
	m_tmp_table_sizes_sum,
	m_tmp_table_sizes_min,
	m_tmp_table_sizes_max,
	m_tmp_table_sizes_p99,
	m_tmp_table_sizes_hg,
	m_qc_hit_sum,
	m_full_scan_sum,
	m_full_join_sum,
	m_tmp_table_sum,
	m_tmp_table_on_disk_sum,
	m_filesort_sum,
	m_filesort_on_disk_sum,
	m_select_full_range_join_sum,
	m_select_range_sum,
	m_select_range_check_sum,
	m_sort_range_sum,
	m_sort_rows_sum,
	m_sort_scan_sum,
	m_no_index_used_sum,
	m_no_good_index_used_sum,
	grpstr,
	grpint,
	labint.key,
	labint.value
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
