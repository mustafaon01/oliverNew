-- Analyse counting all passes data by grouping project file(Project_ID)
EXPLAIN ANALYZE  select bp."Project_ID",
count(distinct bp."LinkingRecord_ID") as num_of_linkingrecords,
count(distinct bp."RenderPass_ID") as num_of_basepasses,
count(distinct op."RenderPass_ID") as num_of_optionpasses
from "RenderPass" bp
        left join "RenderPass" op on op."BasePass_ID" = bp."RenderPass_ID"
where bp."BasePass_ID" is null
group by 1
order by 2;


SELECT state, count(*)
FROM pg_stat_activity
GROUP BY state;


-- if pg_stat_statements will be add to config file
-- shared_preload_libraries = 'pg_stat_statements'

SELECT query, calls, total_exec_time, mean_exec_time, rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;


CREATE EXTENSION pg_stat_statements;


CREATE OR REPLACE VIEW database_performance_summary AS
SELECT
  d.datname AS "DatabaseName",
  numbackends AS "ActiveConnections",
  xact_commit AS "TransactionsCommitted",
  xact_rollback AS "TransactionsRolledBack",
  ROUND((xact_commit::numeric / NULLIF(xact_commit + xact_rollback, 0)), 4) AS success_rate,
  blks_read AS "BlocksRead",
  blks_hit AS "BlocksHit",
  (blks_hit::float / NULLIF(blks_hit + blks_read, 0)) * 100 AS "CacheHitRatioPercentage",
  tup_returned AS "TuplesReturned",
  tup_fetched AS "TuplesFetched",
  tup_inserted AS "TuplesInserted",
  tup_updated AS "TuplesUpdated",
  tup_deleted AS "TuplesDeleted",
  (SELECT checkpoints_timed FROM pg_stat_bgwriter) AS "TimedCheckpoints",
  (SELECT checkpoints_req FROM pg_stat_bgwriter) AS "RequestedCheckpoints",
  (SELECT checkpoint_write_time FROM pg_stat_bgwriter) AS "CheckpointWriteTime_ms",
  (SELECT checkpoint_sync_time FROM pg_stat_bgwriter) AS "CheckpointSyncTime_ms",
  (SELECT buffers_checkpoint FROM pg_stat_bgwriter) AS "BuffersWrittenDuringCheckpoints",
  (SELECT buffers_clean FROM pg_stat_bgwriter) AS "BuffersCleaned",
  (SELECT maxwritten_clean FROM pg_stat_bgwriter) AS "MaxwrittenClean",
  (SELECT buffers_backend FROM pg_stat_bgwriter) AS "BuffersWrittenDirectlyByBackends",
  (SELECT buffers_alloc FROM pg_stat_bgwriter) AS "BuffersAllocated"
FROM
  pg_stat_database d
ORDER BY
  d.datname;


-- Veritabanı Bakımı: Düzenli olarak VACUUM, ANALYZE ve REINDEX işlemlerini gerçekleştirerek
-- veritabanı sağlığını koruyun. Bu işlemler, veritabanı performansını artırabilir ve sorgu optimizasyon
-- planlarının daha doğru olmasını sağlayabilir.



-- same values of our_db
SHOW work_mem; -- if there are so many parallel work, it should be increased.
SHOW shared_buffers;
SHOW maintenance_work_mem;
SHOW effective_cache_size;







