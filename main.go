package main

import (
	"flag"
	"fmt"
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	slowlog "github.com/percona/go-mysql/log"
	parser "github.com/percona/go-mysql/log/slow"
	"github.com/percona/go-mysql/query"
)

var opt = slowlog.Options{}

type closedChannelError struct {
	error
}

func main() {
	slowLogPath := flag.String("slowLogPath", "logs/mysql-slow.log", "Path to MySQL slow log file")
	dsn := flag.String("dsn", "clickhouse://127.0.0.1:9000?database=pmm", "DSN of ClickHouse Server")

	// https://clickhouse.yandex/docs/en/single/#performance-when-inserting-data
	maxRowsPerTx := flag.Int("max-rows-per-tx", 100000, "Maximum rows to commit per ClickHouse transaction.")
	maxTimeForTx := flag.Duration("max-time-for-tx", 5*time.Second, "Maximum time for commit per ClickHouse transaction.")
	newEventWait := flag.Duration("new-event-wait", 10*time.Second, "Time to wait for a new event in slow log.")
	offset := flag.Uint64("offset", 0, "Offset of slowlog")
	flag.Parse()
	opt.StartOffset = *offset

	log.SetOutput(os.Stderr)
	db, err := sqlx.Connect("clickhouse", *dsn)
	if err != nil {
		log.Fatal("Connection: ", err)
	}
	fmt.Println("Connected to ClickHouse DB.")

	events := parseSlowLog(*slowLogPath, opt)
	fmt.Println("Parsing slowlog: ", *slowLogPath, "...")
	for {
		start := time.Now()
		err = transact(db, func(stmt *sqlx.NamedStmt) error {
			i := 0
			for event := range events {
				_, err = stmt.Exec(prepareQueryClassRow(event))
				if err != nil {
					return fmt.Errorf("save error: %v", err)
				}

				// Pass last offset to restart reader when reached out end of slowlog.
				opt.StartOffset = event.OffsetEnd

				i++
				// Commit all executed entities by number or timeout (when slow log is filling rarely)
				// https://clickhouse.yandex/docs/en/single/#performance-when-inserting-data
				if i >= *maxRowsPerTx || time.Since(start) > *maxTimeForTx {
					return nil
				}
			}
			// No new events in slowlog. Nothing to save in ClickHouse.
			if i == 0 {
				return closedChannelError{errors.New("closed channel")}
			}
			// Reached end of slowlog. Save all what we have in ClickHouse.
			return nil
		})

		if err != nil {
			if _, ok := err.(closedChannelError); !ok {
				log.Fatal("transaction error:", err)
			}
			// Channel is closed when reached end of the slowlog.
			// Wait and try read the slowlog again.
			time.Sleep(*newEventWait)
			events = parseSlowLog(*slowLogPath, opt)
		}
	}
}

func prepareQueryClassRow(e *slowlog.Event) queryClassRow {
	fingerprint := query.Fingerprint(e.Query)
	digest := query.Id(fingerprint)

	// Print progress.
	fp := fingerprint
	if len(fingerprint) > 50 {
		fp = fingerprint[:47] + "..."
	}
	fmt.Printf("%-50v  %38v offset: %v:%v\n", fp, e.Ts, e.Offset, e.OffsetEnd)

	return queryClassRow{
		Digest:           digest,
		DigestText:       fingerprint,
		DbSchema:         e.Db,
		DbUsername:       e.User,
		ClientHost:       e.Host,
		PeriodStart:      e.Ts,
		Example:          e.Query,
		MQueryTimeSum:    float32(e.TimeMetrics["Query_time"]),
		MLockTimeSum:     float32(e.TimeMetrics["Lock_time"]),
		MRowsSentSum:     e.NumberMetrics["Rows_sent"],
		MRowsExaminedSum: e.NumberMetrics["Rows_examined"],
		MRowsAffectedSum: e.NumberMetrics["Rows_affected"],
		MBytesSentSum:    e.NumberMetrics["Bytes_sent"],
	}
}

// https://stackoverflow.com/questions/16184238/database-sql-tx-detecting-commit-or-rollback/23502629#23502629
func transact(db *sqlx.DB, txFunc func(*sqlx.NamedStmt) error) (err error) {
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("transaction begin error: %v", err)
	}
	stmt, err := tx.PrepareNamed(insertSQL)
	if err != nil {
		return fmt.Errorf("prepare error: %v", err)
	}
	defer func() {
		if p := recover(); p != nil {
			if err := tx.Rollback(); err!= nil {
				log.Println(err)
			}
			panic(p) // re-throw panic after Rollback
		}

		if err != nil {
			//if err.Error() != "closed channel" {
			if _, ok := err.(closedChannelError); !ok {
				err = fmt.Errorf("unexpected error: %v", err)
				e := tx.Rollback()
				// err is non-nil; append it
				if e != nil {
					err = fmt.Errorf("%v:rollback error: %v", err, e)
				}
			}
			return
		}

		err = stmt.Close()
		if err != nil {
			err = fmt.Errorf("cannot close: %v", err)
		}

		err = tx.Commit() // if Commit returns error update err
		if err != nil {
			err = fmt.Errorf("cannot commit: %v", err)
		}
		fmt.Println("Commit to ClickHouse.")
	}()
	return txFunc(stmt)
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

// queryClassRow reflects Query Class metrics and other attributes
type queryClassRow struct {
	ID                      uint32
	Digest                  string    `db:"digest"`
	DigestText              string    `db:"digest_text"`
	DbServer                string    `db:"db_server"`
	DbSchema                string    `db:"db_schema"`
	DbUsername              string    `db:"db_username"`
	ClientHost              string    `db:"client_host"`
	LabelsKey               []string  `db:"labels_key"`
	LabelsValue             []string  `db:"labels_value"`
	AgentUUID               string    `db:"agent_uuid"`
	PeriodStart             time.Time `db:"period_start"`
	PeriodLength            uint32    `db:"period_length"`
	Example                 string    `db:"example"`
	ExampleFormat           string    `db:"example_format"`
	IsTruncated             uint32    `db:"is_truncated"`
	ExampleType             string    `db:"example_type"`
	ExampleMetrics          string    `db:"example_metrics"`
	NumQueryWithWarnings    uint64    `db:"num_query_with_warnings"`
	WarningsCode            []string  `db:"warnings_code"`
	WarningsCount           []uint64  `db:"warnings_count"`
	NumQueryWithErrors      uint64    `db:"num_query_with_errors"`
	ErrorsCode              []string  `db:"errors_code"`
	ErrorsCount             []uint64  `db:"errors_count"`
	NumQueries              uint64    `db:"num_queries"`
	MQueryTimeCnt           uint32    `db:"m_query_time_cnt"`
	MQueryTimeSum           float32   `db:"m_query_time_sum"`
	MQueryTimeMin           float32   `db:"m_query_time_min"`
	MQueryTimeMax           float32   `db:"m_query_time_max"`
	MQueryTimeP99           float32   `db:"m_query_time_p99"`
	MQueryTimeHg            []uint32  `db:"m_query_time_hg"`
	MLockTimeCnt            uint32    `db:"m_lock_time_cnt"`
	MLockTimeSum            float32   `db:"m_lock_time_sum"`
	MLockTimeMin            float32   `db:"m_lock_time_min"`
	MLockTimeMax            float32   `db:"m_lock_time_max"`
	MLockTimeP99            float32   `db:"m_lock_time_p99"`
	MLockTimeHg             []float32 `db:"m_lock_time_hg"`
	MRowsSentCnt            uint64    `db:"m_rows_sent_cnt"`
	MRowsSentSum            uint64    `db:"m_rows_sent_sum"`
	MRowsSentMin            uint64    `db:"m_rows_sent_min"`
	MRowsSentMax            uint64    `db:"m_rows_sent_max"`
	MRowsSentP99            uint64    `db:"m_rows_sent_p99"`
	MRowsSentHg             []uint64  `db:"m_rows_sent_hg"`
	MRowsExaminedCnt        uint64    `db:"m_rows_examined_cnt"`
	MRowsExaminedSum        uint64    `db:"m_rows_examined_sum"`
	MRowsExaminedMin        uint64    `db:"m_rows_examined_min"`
	MRowsExaminedMax        uint64    `db:"m_rows_examined_max"`
	MRowsExaminedP99        uint64    `db:"m_rows_examined_p99"`
	MRowsExaminedHg         []uint64  `db:"m_rows_examined_hg"`
	MRowsAffectedCnt        uint64    `db:"m_rows_affected_cnt"`
	MRowsAffectedSum        uint64    `db:"m_rows_affected_sum"`
	MRowsAffectedMin        uint64    `db:"m_rows_affected_min"`
	MRowsAffectedMax        uint64    `db:"m_rows_affected_max"`
	MRowsAffectedP99        uint64    `db:"m_rows_affected_p99"`
	MRowsAffectedHg         []uint64  `db:"m_rows_affected_hg"`
	MRowsReadCnt            uint64    `db:"m_rows_read_cnt"`
	MRowsReadSum            uint64    `db:"m_rows_read_sum"`
	MRowsReadMin            uint64    `db:"m_rows_read_min"`
	MRowsReadMax            uint64    `db:"m_rows_read_max"`
	MRowsReadP99            uint64    `db:"m_rows_read_p99"`
	MRowsReadHg             []uint64  `db:"m_rows_read_hg"`
	MMergePassesCnt         uint64    `db:"m_merge_passes_cnt"`
	MMergePassesSum         uint64    `db:"m_merge_passes_sum"`
	MMergePassesMin         uint64    `db:"m_merge_passes_min"`
	MMergePassesMax         uint64    `db:"m_merge_passes_max"`
	MMergePassesP99         uint64    `db:"m_merge_passes_p99"`
	MMergePassesHg          []uint64  `db:"m_merge_passes_hg"`
	MInnodbIoROpsCnt        uint64    `db:"m_innodb_io_r_ops_cnt"`
	MInnodbIoROpsSum        uint64    `db:"m_innodb_io_r_ops_sum"`
	MInnodbIoROpsMin        uint64    `db:"m_innodb_io_r_ops_min"`
	MInnodbIoROpsMax        uint64    `db:"m_innodb_io_r_ops_max"`
	MInnodbIoROpsP99        uint64    `db:"m_innodb_io_r_ops_p99"`
	MInnodbIoROpsHg         []uint64  `db:"m_innodb_io_r_ops_hg"`
	MInnodbIoRBytesCnt      uint64    `db:"m_innodb_io_r_bytes_cnt"`
	MInnodbIoRBytesSum      uint64    `db:"m_innodb_io_r_bytes_sum"`
	MInnodbIoRBytesMin      uint64    `db:"m_innodb_io_r_bytes_min"`
	MInnodbIoRBytesMax      uint64    `db:"m_innodb_io_r_bytes_max"`
	MInnodbIoRBytesP99      uint64    `db:"m_innodb_io_r_bytes_p99"`
	MInnodbIoRBytesHg       []uint64  `db:"m_innodb_io_r_bytes_hg"`
	MInnodbIoRWaitCnt       float32   `db:"m_innodb_io_r_wait_cnt"`
	MInnodbIoRWaitSum       float32   `db:"m_innodb_io_r_wait_sum"`
	MInnodbIoRWaitMin       float32   `db:"m_innodb_io_r_wait_min"`
	MInnodbIoRWaitMax       float32   `db:"m_innodb_io_r_wait_max"`
	MInnodbIoRWaitP99       float32   `db:"m_innodb_io_r_wait_p99"`
	MInnodbIoRWaitHg        []float32 `db:"m_innodb_io_r_wait_hg"`
	MInnodbRecLockWaitCnt   float32   `db:"m_innodb_rec_lock_wait_cnt"`
	MInnodbRecLockWaitSum   float32   `db:"m_innodb_rec_lock_wait_sum"`
	MInnodbRecLockWaitMin   float32   `db:"m_innodb_rec_lock_wait_min"`
	MInnodbRecLockWaitMax   float32   `db:"m_innodb_rec_lock_wait_max"`
	MInnodbRecLockWaitP99   float32   `db:"m_innodb_rec_lock_wait_p99"`
	MInnodbRecLockWaitHg    []float32 `db:"m_innodb_rec_lock_wait_hg"`
	MInnodbQueueWaitCnt     float32   `db:"m_innodb_queue_wait_cnt"`
	MInnodbQueueWaitSum     float32   `db:"m_innodb_queue_wait_sum"`
	MInnodbQueueWaitMin     float32   `db:"m_innodb_queue_wait_min"`
	MInnodbQueueWaitMax     float32   `db:"m_innodb_queue_wait_max"`
	MInnodbQueueWaitP99     float32   `db:"m_innodb_queue_wait_p99"`
	MInnodbQueueWaitHg      []float32 `db:"m_innodb_queue_wait_hg"`
	MInnodbPagesDistinctCnt uint64    `db:"m_innodb_pages_distinct_cnt"`
	MInnodbPagesDistinctSum uint64    `db:"m_innodb_pages_distinct_sum"`
	MInnodbPagesDistinctMin uint64    `db:"m_innodb_pages_distinct_min"`
	MInnodbPagesDistinctMax uint64    `db:"m_innodb_pages_distinct_max"`
	MInnodbPagesDistinctP99 uint64    `db:"m_innodb_pages_distinct_p99"`
	MInnodbPagesDistinctHg  []uint64  `db:"m_innodb_pages_distinct_hg"`
	MQueryLengthCnt         uint64    `db:"m_query_length_cnt"`
	MQueryLengthSum         uint64    `db:"m_query_length_sum"`
	MQueryLengthMin         uint64    `db:"m_query_length_min"`
	MQueryLengthMax         uint64    `db:"m_query_length_max"`
	MQueryLengthP99         uint64    `db:"m_query_length_p99"`
	MQueryLengthHg          []uint64  `db:"m_query_length_hg"`
	MBytesSentCnt           uint64    `db:"m_bytes_sent_cnt"`
	MBytesSentSum           uint64    `db:"m_bytes_sent_sum"`
	MBytesSentMin           uint64    `db:"m_bytes_sent_min"`
	MBytesSentMax           uint64    `db:"m_bytes_sent_max"`
	MBytesSentP99           uint64    `db:"m_bytes_sent_p99"`
	MBytesSentHg            []uint64  `db:"m_bytes_sent_hg"`
	MTmpTablesCnt           uint64    `db:"m_tmp_tables_cnt"`
	MTmpTablesSum           uint64    `db:"m_tmp_tables_sum"`
	MTmpTablesMin           uint64    `db:"m_tmp_tables_min"`
	MTmpTablesMax           uint64    `db:"m_tmp_tables_max"`
	MTmpTablesP99           uint64    `db:"m_tmp_tables_p99"`
	MTmpTablesHg            []uint64  `db:"m_tmp_tables_hg"`
	MTmpDiskTablesCnt       uint64    `db:"m_tmp_disk_tables_cnt"`
	MTmpDiskTablesSum       uint64    `db:"m_tmp_disk_tables_sum"`
	MTmpDiskTablesMin       uint64    `db:"m_tmp_disk_tables_min"`
	MTmpDiskTablesMax       uint64    `db:"m_tmp_disk_tables_max"`
	MTmpDiskTablesP99       uint64    `db:"m_tmp_disk_tables_p99"`
	MTmpDiskTablesHg        []uint64  `db:"m_tmp_disk_tables_hg"`
	MTmpTableSizesCnt       uint64    `db:"m_tmp_table_sizes_cnt"`
	MTmpTableSizesSum       uint64    `db:"m_tmp_table_sizes_sum"`
	MTmpTableSizesMin       uint64    `db:"m_tmp_table_sizes_min"`
	MTmpTableSizesMax       uint64    `db:"m_tmp_table_sizes_max"`
	MTmpTableSizesP99       uint64    `db:"m_tmp_table_sizes_p99"`
	MTmpTableSizesHg        []uint64  `db:"m_tmp_table_sizes_hg"`
	MQcHitSum               uint64    `db:"m_qc_hit_sum"`
	MFullScanSum            uint64    `db:"m_full_scan_sum"`
	MFullJoinSum            uint64    `db:"m_full_join_sum"`
	MTmpTableSum            uint64    `db:"m_tmp_table_sum"`
	MTmpTableOnDiskSum      uint64    `db:"m_tmp_table_on_disk_sum"`
	MFilesortSum            uint64    `db:"m_filesort_sum"`
	MFilesortOnDiskSum      uint64    `db:"m_filesort_on_disk_sum"`
	MSelectFullRangeJoinSum uint64    `db:"m_select_full_range_join_sum"`
	MSelectRangeSum         uint64    `db:"m_select_range_sum"`
	MSelectRangeCheckSum    uint64    `db:"m_select_range_check_sum"`
	MSortRangeSum           uint64    `db:"m_sort_range_sum"`
	MSortRowsSum            uint64    `db:"m_sort_rows_sum"`
	MSortScanSum            uint64    `db:"m_sort_scan_sum"`
	MNoIndexUsedSum         uint64    `db:"m_no_index_used_sum"`
	MNoGoodIndexUsedSum     uint64    `db:"m_no_good_index_used_sum"`
	Grpstr                  string    `db:"grpstr"`
	Grpint                  uint32    `db:"grpint"`
	LabintKey               []uint32  `db:"labint_key"`
	LabintValue             []uint32  `db:"labint_value"`
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
	num_query_with_warnings,
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
   )
  VALUES (
	:digest,
	:digest_text,
	:db_server,
	:db_schema,
	:db_username,
	:client_host,
	:labels_key,
	:labels_value,
	:agent_uuid,
	:period_start,
	:period_length,
	:example,
	:is_truncated,
	:example_metrics,
	:num_query_with_warnings,
	:warnings_code,
	:warnings_count,
	:num_query_with_errors,
	:errors_code,
	:errors_count,
	:num_queries,
	:m_query_time_cnt,
	:m_query_time_sum,
	:m_query_time_min,
	:m_query_time_max,
	:m_query_time_p99,
	:m_query_time_hg,
	:m_lock_time_cnt,
	:m_lock_time_sum,
	:m_lock_time_min,
	:m_lock_time_max,
	:m_lock_time_p99,
	:m_lock_time_hg,
	:m_rows_sent_cnt,
	:m_rows_sent_sum,
	:m_rows_sent_min,
	:m_rows_sent_max,
	:m_rows_sent_p99,
	:m_rows_sent_hg,
	:m_rows_examined_cnt,
	:m_rows_examined_sum,
	:m_rows_examined_min,
	:m_rows_examined_max,
	:m_rows_examined_p99,
	:m_rows_examined_hg,
	:m_rows_affected_cnt,
	:m_rows_affected_sum,
	:m_rows_affected_min,
	:m_rows_affected_max,
	:m_rows_affected_p99,
	:m_rows_affected_hg,
	:m_rows_read_cnt,
	:m_rows_read_sum,
	:m_rows_read_min,
	:m_rows_read_max,
	:m_rows_read_p99,
	:m_rows_read_hg,
	:m_merge_passes_cnt,
	:m_merge_passes_sum,
	:m_merge_passes_min,
	:m_merge_passes_max,
	:m_merge_passes_p99,
	:m_merge_passes_hg,
	:m_innodb_io_r_ops_cnt,
	:m_innodb_io_r_ops_sum,
	:m_innodb_io_r_ops_min,
	:m_innodb_io_r_ops_max,
	:m_innodb_io_r_ops_p99,
	:m_innodb_io_r_ops_hg,
	:m_innodb_io_r_bytes_cnt,
	:m_innodb_io_r_bytes_sum,
	:m_innodb_io_r_bytes_min,
	:m_innodb_io_r_bytes_max,
	:m_innodb_io_r_bytes_p99,
	:m_innodb_io_r_bytes_hg,
	:m_innodb_io_r_wait_cnt,
	:m_innodb_io_r_wait_sum,
	:m_innodb_io_r_wait_min,
	:m_innodb_io_r_wait_max,
	:m_innodb_io_r_wait_p99,
	:m_innodb_io_r_wait_hg,
	:m_innodb_rec_lock_wait_cnt,
	:m_innodb_rec_lock_wait_sum,
	:m_innodb_rec_lock_wait_min,
	:m_innodb_rec_lock_wait_max,
	:m_innodb_rec_lock_wait_p99,
	:m_innodb_rec_lock_wait_hg,
	:m_innodb_queue_wait_cnt,
	:m_innodb_queue_wait_sum,
	:m_innodb_queue_wait_min,
	:m_innodb_queue_wait_max,
	:m_innodb_queue_wait_p99,
	:m_innodb_queue_wait_hg,
	:m_innodb_pages_distinct_cnt,
	:m_innodb_pages_distinct_sum,
	:m_innodb_pages_distinct_min,
	:m_innodb_pages_distinct_max,
	:m_innodb_pages_distinct_p99,
	:m_innodb_pages_distinct_hg,
	:m_query_length_cnt,
	:m_query_length_sum,
	:m_query_length_min,
	:m_query_length_max,
	:m_query_length_p99,
	:m_query_length_hg,
	:m_bytes_sent_cnt,
	:m_bytes_sent_sum,
	:m_bytes_sent_min,
	:m_bytes_sent_max,
	:m_bytes_sent_p99,
	:m_bytes_sent_hg,
	:m_tmp_tables_cnt,
	:m_tmp_tables_sum,
	:m_tmp_tables_min,
	:m_tmp_tables_max,
	:m_tmp_tables_p99,
	:m_tmp_tables_hg,
	:m_tmp_disk_tables_cnt,
	:m_tmp_disk_tables_sum,
	:m_tmp_disk_tables_min,
	:m_tmp_disk_tables_max,
	:m_tmp_disk_tables_p99,
	:m_tmp_disk_tables_hg,
	:m_tmp_table_sizes_cnt,
	:m_tmp_table_sizes_sum,
	:m_tmp_table_sizes_min,
	:m_tmp_table_sizes_max,
	:m_tmp_table_sizes_p99,
	:m_tmp_table_sizes_hg,
	:m_qc_hit_sum,
	:m_full_scan_sum,
	:m_full_join_sum,
	:m_tmp_table_sum,
	:m_tmp_table_on_disk_sum,
	:m_filesort_sum,
	:m_filesort_on_disk_sum,
	:m_select_full_range_join_sum,
	:m_select_range_sum,
	:m_select_range_check_sum,
	:m_sort_range_sum,
	:m_sort_rows_sum,
	:m_sort_scan_sum,
	:m_no_index_used_sum,
	:m_no_good_index_used_sum,
	:grpstr,
	:grpint,
	:labint_key
	:labint_value
  )
`
