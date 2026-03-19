------------------------------------------------------------------------------
-- Procedure Name : PRC_WOT_TO_STG_SYNC
-- Purpose        : Synchronize WORKORDERTASK data to EPSEWERAI_WOT_STG.
--                  This procedure ensures staging updates occur even when
--                  WORKORDERTASK rows are loaded using direct-path inserts
--                  from the GeoFIT > Oracle > Ivara integration.
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

-- Correction     :
--   • Removed legacy logic that defaulted invalid WORKCLASSIFI_OI to 209.
--   • Added classification filter identical to trigger early-exit logic.
--   • Ensures only approved classifications are processed.
------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRC_WOT_TO_STG_SYNC AS
  c_site_oi CONSTANT NUMBER := 58;
BEGIN

  ----------------------------------------------------------------------------
  -- MERGE: Process only tasks updated in the last 5 minutes AND with
  --        allowed classifications.
  ----------------------------------------------------------------------------
  MERGE INTO CUSTOMERDATA.EPSEWERAI_WOT_STG s
  USING (
    SELECT
      wo.WONUMBER                                         AS wonumber,
      wo.UUID                                             AS workorder_uuid,
      t.UUID                                              AS task_uuid,
      t.TASKNUMBER                                        AS tasknumber,

      -- Prefer task title; fallback to WO title
      NVL(t.WOTASKTITLE, wo.TITLE)                        AS wotasktitle,

      -- Planned completion comes from WORKORDERS only
      wo.REQCOMPDATE_DTTM                                 AS plndcompdate_dttm,

      -- Prefer task-level start; fallback to WO-level
      NVL(t.PLNDSTRTDATE_DTTM, wo.PLNDSTRTDATE_DTTM)      AS plndstrtdate_dttm,

      -- Use the actual classification (NO defaulting to 209)
      t.WORKCLASSIFI_OI                                   AS workclassifi_oi

    FROM MNT.WORKORDERTASK t
    JOIN MNT.WORKORDERS wo
      ON wo.WORKORDERSOI = t.WORKORDER_OI
     AND wo.SITE_OI      = c_site_oi

    -- Only process rows updated in last 5 minutes
    -- AND with allowed classification codes
    WHERE t.LASTUPDATE_DTTM >= SYSDATE - (5/1440)
      AND t.WORKCLASSIFI_OI IN (
            209, 211, 215, 266, 442, 462,
            183, 196, 207, 256, 263
          )
  ) src
  ON (s.TASK_UUID = src.TASK_UUID)

  ----------------------------------------------------------------------------
  -- WHEN MATCHED: Update only when values changed.
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
    WHERE     NVL(s.WONUMBER, '~') <> NVL(src.WONUMBER, '~')
           OR NVL(s.TASKNUMBER, -1) <> NVL(src.TASKNUMBER, -1)
           OR NVL(s.WOTASKTITLE, '~') <> NVL(src.WOTASKTITLE, '~')
           OR NVL(s.PLNDCOMPDATE_DTTM, DATE '1900-01-01')
              <> NVL(src.PLNDCOMPDATE_DTTM, DATE '1900-01-01')
           OR NVL(s.PLNDSTRTDATE_DTTM, DATE '1900-01-01')
              <> NVL(src.PLNDSTRTDATE_DTTM, DATE '1900-01-01')
           OR NVL(s.WORKCLASSIFI_OI, -1) <> NVL(src.WORKCLASSIFI_OI, -1)

  ----------------------------------------------------------------------------
  -- WHEN NOT MATCHED: Insert new rows.
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
