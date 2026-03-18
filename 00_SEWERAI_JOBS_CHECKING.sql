------------------------------------------------------------------------------
-- Shows each execution of the job, including:
--   • Start time
--   • End time
--   • Duration
--   • Status (SUCCEEDED / FAILED / RETRY)
--   • Error messages
--
-- ORDER BY most recent run first.
------------------------------------------------------------------------------

SELECT 
    log_id,                         -- Unique ID for each run
    job_name,                       -- Should be JOB_WOT_TO_STG_SYNC
    status,                         -- SUCCEEDED / FAILED / RETRY
    error#,                         -- Oracle error code (NULL if success)
    additional_info,                -- Text describing failure (if any)
    TO_CHAR(req_start_date, 'YYYY-MM-DD HH24:MI:SS') AS requested_start,
    TO_CHAR(actual_start_date, 'YYYY-MM-DD HH24:MI:SS') AS actual_start,
    run_duration                    -- How long the job took
FROM dba_scheduler_job_run_details
WHERE job_name = 'JOB_WOT_TO_STG_SYNC'
ORDER BY log_id DESC;



------------------------------------------------------------------------------
-- Shows job status, next run time, and scheduling details
------------------------------------------------------------------------------

SELECT
    job_name,             -- JOB_WOT_TO_STG_SYNC
    state,                -- ENABLED / DISABLED / RUNNING
    next_run_date,        -- When the next execution will occur
    last_start_date,      -- When it last began
    last_run_duration,    -- Last run time (HH:MI:SS)
    failure_count,        -- How many times the job has failed
    run_count             -- How many total executions have occurred
FROM dba_scheduler_jobs
WHERE job_name = 'JOB_WOT_TO_STG_SYNC';


------------------------------------------------------------------------------
-- Shows if the job is running RIGHT NOW (useful for debugging)
------------------------------------------------------------------------------

SELECT 
    s.session_id,
    s.running_instance,
    s.elapsed_time,
    j.job_name
FROM dba_scheduler_running_jobs s
JOIN dba_scheduler_jobs j
  ON j.job_name = s.job_name
WHERE s.job_name = 'JOB_WOT_TO_STG_SYNC';


SELECT
    TASK_UUID,
    WONUMBER,
    TASKNUMBER,
    FEED_STATUS
FROM CUSTOMERDATA.EPSEWERAI_WOT_STG
WHERE FEED_STATUS IN ('NEW','UPDATED')
ORDER BY TASK_UUID DESC;