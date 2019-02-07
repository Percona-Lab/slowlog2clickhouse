/* Query total values to calculate percentage */

SELECT
    SUM(num_queries) AS num_queries,
    SUM(m_query_time_cnt) AS m_query_time_cnt,
    SUM(m_query_time_sum) AS m_query_time_sum,
    MIN(m_query_time_min) AS m_query_time_min,
    MAX(m_query_time_max) AS m_query_time_max,
    AVG(m_query_time_p99) AS m_query_time_p99
FROM queries
WHERE (period_start > '2019-01-11 00:00:03')
AND (period_start < '2019-01-17 23:59:51')
AND (db_server IN ('db1', 'db2'))
AND has(['label1', 'label2'], labels.value[indexOf(labels.key, 'key5')])

/* Metrics by query classes with filtering. */

SELECT
    digest,
    any(digest_text) AS digest_text,
    MIN(period_start) AS first_seen,
    MAX(period_start) AS last_seen,
    SUM(num_queries) AS num_queries,
    SUM(m_query_time_cnt) AS m_query_time_cnt,
    SUM(m_query_time_sum) AS m_query_time_sum,
    MIN(m_query_time_min) AS m_query_time_min,
    MAX(m_query_time_max) AS m_query_time_max,
    AVG(m_query_time_p99) AS m_query_time_p99
FROM queries
WHERE (period_start > '2019-01-11 00:00:03')
AND (period_start < '2019-01-17 23:59:51')
AND (db_server IN ('db1', 'db2'))
AND has(['label1', 'label2'], labels.value[indexOf(labels.key, 'key5')])
GROUP BY digest
ORDER BY m_query_time_sum DESC
LIMIT 10 OFFSET 20;

/* Metrics by schema with filtering. */

SELECT
    db_schema,
    SUM(num_queries) AS num_queries,
    SUM(m_query_time_cnt) AS m_query_time_cnt,
    SUM(m_query_time_sum) AS m_query_time_sum,
    MIN(m_query_time_min) AS m_query_time_min,
    MAX(m_query_time_max) AS m_query_time_max,
    AVG(m_query_time_p99) AS m_query_time_p99
FROM queries
WHERE (period_start > '2019-01-11 00:00:03')
AND (period_start < '2019-01-17 23:59:51')
AND (db_server IN ('db1', 'db2'))
AND has(['label1', 'label2'], labels.value[indexOf(labels.key, 'key5')])
GROUP BY db_schema
ORDER BY m_query_time_sum DESC
LIMIT 10 OFFSET 20;

/* Metrics by db server with filtering. */

SELECT
    db_server,
    SUM(num_queries) AS num_queries,
    SUM(m_query_time_cnt) AS m_query_time_cnt,
    SUM(m_query_time_sum) AS m_query_time_sum,
    MIN(m_query_time_min) AS m_query_time_min,
    MAX(m_query_time_max) AS m_query_time_max,
    AVG(m_query_time_p99) AS m_query_time_p99
FROM queries
WHERE (period_start > '2019-01-11 00:00:03')
AND (period_start < '2019-01-17 23:59:51')
AND (db_server IN ('db1', 'db2'))
AND has(['label1', 'label2'], labels.value[indexOf(labels.key, 'key5')])
GROUP BY db_server
ORDER BY m_query_time_sum DESC
LIMIT 10 OFFSET 20;

/* All metrics for given digest with filtering. */

SELECT
digest,
any(digest_text) AS digest_text,
groupUniqArray(db_server) AS db_servers,
groupUniqArray(db_schema) AS db_schemas,
groupUniqArray(db_username) AS db_usernames,
groupUniqArray(client_host) AS client_hosts,

MIN(period_start) AS first_seen,
MAX(period_start) AS last_seen,

SUM(num_queries) AS num_queries,

SUM(m_query_time_cnt) AS m_query_time_cnt,
SUM(m_query_time_sum) AS m_query_time_sum,
MIN(m_query_time_min) AS m_query_time_min,
MAX(m_query_time_max) AS m_query_time_max,
AVG(m_query_time_p99) AS m_query_time_p99,

SUM(m_lock_time_cnt) AS m_lock_time_cnt,
SUM(m_lock_time_sum) AS m_lock_time_sum,
MIN(m_lock_time_min) AS m_lock_time_min,
MAX(m_lock_time_max) AS m_lock_time_max,
AVG(m_lock_time_p99) AS m_lock_time_p99,

SUM(m_rows_sent_cnt) AS m_rows_sent_cnt,
SUM(m_rows_sent_sum) AS m_rows_sent_sum,
MIN(m_rows_sent_min) AS m_rows_sent_min,
MAX(m_rows_sent_max) AS m_rows_sent_max,
AVG(m_rows_sent_p99) AS m_rows_sent_p99,

SUM(m_rows_examined_cnt) AS m_rows_examined_cnt,
SUM(m_rows_examined_sum) AS m_rows_examined_sum,
MIN(m_rows_examined_min) AS m_rows_examined_min,
MAX(m_rows_examined_max) AS m_rows_examined_max,
AVG(m_rows_examined_p99) AS m_rows_examined_p99,

SUM(m_rows_affected_cnt) AS m_rows_affected_cnt,
SUM(m_rows_affected_sum) AS m_rows_affected_sum,
MIN(m_rows_affected_min) AS m_rows_affected_min,
MAX(m_rows_affected_max) AS m_rows_affected_max,
AVG(m_rows_affected_p99) AS m_rows_affected_p99,

SUM(m_rows_read_cnt) AS m_rows_read_cnt,
SUM(m_rows_read_sum) AS m_rows_read_sum,
MIN(m_rows_read_min) AS m_rows_read_min,
MAX(m_rows_read_max) AS m_rows_read_max,
AVG(m_rows_read_p99) AS m_rows_read_p99,

SUM(m_merge_passes_cnt) AS m_merge_passes_cnt,
SUM(m_merge_passes_sum) AS m_merge_passes_sum,
MIN(m_merge_passes_min) AS m_merge_passes_min,
MAX(m_merge_passes_max) AS m_merge_passes_max,
AVG(m_merge_passes_p99) AS m_merge_passes_p99,

SUM(m_innodb_io_r_ops_cnt) AS m_innodb_io_r_ops_cnt,
SUM(m_innodb_io_r_ops_sum) AS m_innodb_io_r_ops_sum,
MIN(m_innodb_io_r_ops_min) AS m_innodb_io_r_ops_min,
MAX(m_innodb_io_r_ops_max) AS m_innodb_io_r_ops_max,
AVG(m_innodb_io_r_ops_p99) AS m_innodb_io_r_ops_p99,

SUM(m_innodb_io_r_bytes_cnt) AS m_innodb_io_r_bytes_cnt,
SUM(m_innodb_io_r_bytes_sum) AS m_innodb_io_r_bytes_sum,
MIN(m_innodb_io_r_bytes_min) AS m_innodb_io_r_bytes_min,
MAX(m_innodb_io_r_bytes_max) AS m_innodb_io_r_bytes_max,
AVG(m_innodb_io_r_bytes_p99) AS m_innodb_io_r_bytes_p99,

SUM(m_innodb_io_r_wait_cnt) AS m_innodb_io_r_wait_cnt,
SUM(m_innodb_io_r_wait_sum) AS m_innodb_io_r_wait_sum,
MIN(m_innodb_io_r_wait_min) AS m_innodb_io_r_wait_min,
MAX(m_innodb_io_r_wait_max) AS m_innodb_io_r_wait_max,
AVG(m_innodb_io_r_wait_p99) AS m_innodb_io_r_wait_p99,

SUM(m_innodb_rec_lock_wait_cnt) AS m_innodb_rec_lock_wait_cnt,
SUM(m_innodb_rec_lock_wait_sum) AS m_innodb_rec_lock_wait_sum,
MIN(m_innodb_rec_lock_wait_min) AS m_innodb_rec_lock_wait_min,
MAX(m_innodb_rec_lock_wait_max) AS m_innodb_rec_lock_wait_max,
AVG(m_innodb_rec_lock_wait_p99) AS m_innodb_rec_lock_wait_p99,

SUM(m_innodb_queue_wait_cnt) AS m_innodb_queue_wait_cnt,
SUM(m_innodb_queue_wait_sum) AS m_innodb_queue_wait_sum,
MIN(m_innodb_queue_wait_min) AS m_innodb_queue_wait_min,
MAX(m_innodb_queue_wait_max) AS m_innodb_queue_wait_max,
AVG(m_innodb_queue_wait_p99) AS m_innodb_queue_wait_p99,

SUM(m_innodb_pages_distinct_cnt) AS m_innodb_pages_distinct_cnt,
SUM(m_innodb_pages_distinct_sum) AS m_innodb_pages_distinct_sum,
MIN(m_innodb_pages_distinct_min) AS m_innodb_pages_distinct_min,
MAX(m_innodb_pages_distinct_max) AS m_innodb_pages_distinct_max,
AVG(m_innodb_pages_distinct_p99) AS m_innodb_pages_distinct_p99,

SUM(m_query_length_cnt) AS m_query_length_cnt,
SUM(m_query_length_sum) AS m_query_length_sum,
MIN(m_query_length_min) AS m_query_length_min,
MAX(m_query_length_max) AS m_query_length_max,
AVG(m_query_length_p99) AS m_query_length_p99,

SUM(m_bytes_sent_cnt) AS m_bytes_sent_cnt,
SUM(m_bytes_sent_sum) AS m_bytes_sent_sum,
MIN(m_bytes_sent_min) AS m_bytes_sent_min,
MAX(m_bytes_sent_max) AS m_bytes_sent_max,
AVG(m_bytes_sent_p99) AS m_bytes_sent_p99,

SUM(m_tmp_tables_cnt) AS m_tmp_tables_cnt,
SUM(m_tmp_tables_sum) AS m_tmp_tables_sum,
MIN(m_tmp_tables_min) AS m_tmp_tables_min,
MAX(m_tmp_tables_max) AS m_tmp_tables_max,
AVG(m_tmp_tables_p99) AS m_tmp_tables_p99,

SUM(m_tmp_disk_tables_cnt) AS m_tmp_disk_tables_cnt,
SUM(m_tmp_disk_tables_sum) AS m_tmp_disk_tables_sum,
MIN(m_tmp_disk_tables_min) AS m_tmp_disk_tables_min,
MAX(m_tmp_disk_tables_max) AS m_tmp_disk_tables_max,
AVG(m_tmp_disk_tables_p99) AS m_tmp_disk_tables_p99,

SUM(m_tmp_table_sizes_cnt) AS m_tmp_table_sizes_cnt,
SUM(m_tmp_table_sizes_sum) AS m_tmp_table_sizes_sum,
MIN(m_tmp_table_sizes_min) AS m_tmp_table_sizes_min,
MAX(m_tmp_table_sizes_max) AS m_tmp_table_sizes_max,
AVG(m_tmp_table_sizes_p99) AS m_tmp_table_sizes_p99,

SUM(m_qc_hit_sum) AS m_qc_hit_sum,
SUM(m_full_scan_sum) AS m_full_scan_sum,
SUM(m_full_join_sum) AS m_full_join_sum,
SUM(m_tmp_table_sum) AS m_tmp_table_sum,
SUM(m_tmp_table_on_disk_sum) AS m_tmp_table_on_disk_sum,
SUM(m_filesort_sum) AS m_filesort_sum,
SUM(m_filesort_on_disk_sum) AS m_filesort_on_disk_sum,
SUM(m_select_full_range_join_sum) AS m_select_full_range_join_sum,
SUM(m_select_range_sum) AS m_select_range_sum,
SUM(m_select_range_check_sum) AS m_select_range_check_sum,
SUM(m_sort_range_sum) AS m_sort_range_sum,
SUM(m_sort_rows_sum) AS m_sort_rows_sum,
SUM(m_sort_scan_sum) AS m_sort_scan_sum,
SUM(m_no_index_used_sum) AS m_no_index_used_sum,
SUM(m_no_good_index_used_sum) AS m_no_good_index_used_sum
FROM queries
WHERE (period_start > '2019-01-11 00:00:03')
AND (period_start < '2019-01-17 23:59:51')
AND digest = '1D410B4BE5060972'
AND (db_server IN ('db1', 'db2'))
AND has(['label1', 'label2'], labels.value[indexOf(labels.key, 'key5')])
GROUP BY digest;
