------------------------------------------------------------------------------
-- Procedure Name : PRC_WOT_TO_STG_SYNC
-- Purpose        : Synchronize WORKORDERTASK data to EPSEWERAI_WOT_STG.
--                  This procedure ensures staging updates occur even when
--                  WORKORDERTASK rows are loaded using direct-path inserts
--                  from the GeoFIT ? IST2 ? Ivara integration.
--
-- Why Needed     :
--   • Direct?path inserts bypass the TRG_WOT_TO_STG row?level trigger.
--   • This procedure is executed every 5 minutes by DBMS_SCHEDULER to
--     capture any missed inserts/updates and keep the staging table accurate.
--
-- Logic Summary  :
--   1. Pull WORKORDERTASK rows updated in the last 5 minutes.
--   2. Join to WORKORDERS to fetch required WO?level fields.
--   3. Apply same business rules as the original trigger.
--   4. MERGE into EPSEWERAI_WOT_STG.
------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRC_WOT_TO_STG_SYNC AS
  -- Site constant used in both trigger and procedure (keeps logic consistent)
  c_site_oi CONSTANT NUMBER := 58;
BEGIN

  ----------------------------------------------------------------------------
  -- MERGE: Process only tasks updated within the last 5 minutes.
  -- This matches the scheduler frequency and avoids scanning the whole table.
  ----------------------------------------------------------------------------
  MERGE INTO CUSTOMERDATA.EPSEWERAI_WOT_STG s
  USING (
    SELECT
      wo.WONUMBER                                         AS wonumber,
      wo.UUID                                             AS workorder_uuid,
      t.UUID                                              AS task_uuid,
      t.TASKNUMBER                                        AS tasknumber,

      -- Prefer task title; fallback to work order title if missing
      NVL(t.WOTASKTITLE, wo.TITLE)                        AS wotasktitle,

      -- Business rule: Planned completion date comes ONLY from WORKORDERS
      wo.REQCOMPDATE_DTTM                                 AS plndcompdate_dttm,

      -- Prefer task start date; otherwise inherit from WORKORDERS
      NVL(t.PLNDSTRTDATE_DTTM, wo.PLNDSTRTDATE_DTTM)      AS plndstrtdate_dttm,

      -- WORKCLASSIFI_OI allowed values; default to 209 if invalid
      CASE
        WHEN t.WORKCLASSIFI_OI IN (209, 211, 215, 266, 442, 462,
                                   183, 196, 207, 256, 263)
          THEN t.WORKCLASSIFI_OI
        ELSE 209
      END                                                 AS workclassifi_oi

    FROM MNT.WORKORDERTASK t
    JOIN MNT.WORKORDERS wo
      ON wo.WORKORDERSOI = t.WORKORDER_OI
     AND wo.SITE_OI      = c_site_oi

    -- Corrected column: LASTUPDATE_DTTM (not LAST_UPDATE_DTTM)
    -- Only process tasks updated in the last 5 minutes.
    WHERE t.LASTUPDATE_DTTM >= SYSDATE - (5/1440)

  ) src

  -- Match logic: same TASK_UUID means same target row
  ON (s.TASK_UUID = src.TASK_UUID)

  ----------------------------------------------------------------------------
  -- WHEN MATCHED: Update ONLY when values have changed.
  -- This avoids unnecessary writes, redo, and downstream churn.
  ----------------------------------------------------------------------------
  WHEN MATCHED THEN
    UPDATE SET
      s.WORKORDER_UUID     = src.WORKORDER_UUID,
      s.WONUMBER           = src.WONUMBER,
      s.TASKNUMBER         = src.TASKNUMBER,
      s.WOTASKTITLE        = src.WOTASKTITLE,
      s.PLNDCOMPDATE_DTTM  = src.PLNDCOMPDATE_DTTM,
      s.PLNDSTRTDATE_DTTM  = src.PLNDSTRTDATE_DTTM,
      s.WORKCLASSIFI_OI    = src.WORKCLASSIFI_OI,
      s.FEED_STATUS        = 'UPDATED'
    WHERE
         NVL(s.WONUMBER, '~') <> NVL(src.WONUMBER, '~')
      OR NVL(s.TASKNUMBER, -1) <> NVL(src.TASKNUMBER, -1)
      OR NVL(s.WOTASKTITLE, '~') <> NVL(src.WOTASKTITLE, '~')
      OR NVL(s.PLNDCOMPDATE_DTTM, DATE '1900-01-01')
         <> NVL(src.PLNDCOMPDATE_DTTM, DATE '1900-01-01')
      OR NVL(s.PLNDSTRTDATE_DTTM, DATE '1900-01-01')
         <> NVL(src.PLNDSTRTDATE_DTTM, DATE '1900-01-01')
      OR NVL(s.WORKCLASSIFI_OI, -1) <> NVL(src.WORKCLASSIFI_OI, -1)

  ----------------------------------------------------------------------------
  -- WHEN NOT MATCHED: Insert new rows into staging.
  ----------------------------------------------------------------------------
  WHEN NOT MATCHED THEN
    INSERT (
      TASK_UUID,
      WORKORDER_UUID,
      WONUMBER,
      TASKNUMBER,
      WOTASKTITLE,
      PLNDCOMPDATE_DTTM,
      PLNDSTRTDATE_DTTM,
      WORKCLASSIFI_OI,
      FEED_STATUS
    )
    VALUES (
      src.TASK_UUID,
      src.WORKORDER_UUID,
      src.WONUMBER,
      src.TASKNUMBER,
      src.WOTASKTITLE,
      src.PLNDCOMPDATE_DTTM,
      src.PLNDSTRTDATE_DTTM,
      src.WORKCLASSIFI_OI,
      'NEW'
    );

END PRC_WOT_TO_STG_SYNC;