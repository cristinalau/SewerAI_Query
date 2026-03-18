------------------------------------------------------------------------------
-- JOB: JOB_WOT_TO_STG_SYNC
-- PURPOSE:
--   • Runs every 5 minutes
--   • Executes PRC_WOT_TO_STG_SYNC
--   • Captures WORKORDERTASK rows loaded through bulk/direct-path methods
--   • Ensures staging table stays synchronized even when triggers are bypassed
--
-- NOTES:
--   • DBMS_SCHEDULER is preferred over DBMS_JOB (modern, reliable, monitored)
--   • repeat_interval uses calendaring syntax for precise frequency
------------------------------------------------------------------------------

BEGIN
  DBMS_SCHEDULER.CREATE_JOB (
    job_name        => 'JOB_WOT_TO_STG_SYNC',     -- Logical name of the job
    job_type        => 'STORED_PROCEDURE',        -- Calls a stored procedure
    job_action      => 'PRC_WOT_TO_STG_SYNC',     -- Procedure to execute
    start_date      => SYSTIMESTAMP,              -- Start immediately
    repeat_interval => 'FREQ=MINUTELY; INTERVAL=5', -- Run every 5 minutes
    enabled         => TRUE,                      -- Activate job
    comments        => 'Synchronizes WORKORDERTASK to staging every 5 minutes.'
  );
END;
