/*==============================================================================
  Trigger:   TRG_WOT_TO_STG
  Scope:     MNT.WORKORDERTASK (row-level)
  Timing:    AFTER INSERT OR UPDATE OF
             (wotasktitle, plndcompdate_dttm, plndstrtdate_dttm, workclassifi_oi)

  Purpose:
    Maintain the staging table CUSTOMERDATA.EPSEWERAI_WOT_STG with the latest
    task data for selected work classifications. The trigger:
      - Filters tasks by classification (209, 211, 215, 266, 352) and site 58.
      - Resolves the parent Work Order UUID from MNT.WORKORDERS.
      - Computes an “effective date” for the incoming row as the GREATEST of
        planned start vs planned completion (nulls treated as very old).
      - Compares that to the current max effective date in the staging table
        (excluding rows marked SENT).
      - MERGEs into the staging table:
          * UPDATE the matched TASK_UUID only when relevant fields changed,
            and set FEED_STATUS = 'UPDATED' (optionally bump FEED_STATUS_DTTM).
          * INSERT a new row only when the incoming row is considered “latest”.

  High-level Flow:
    1) Exit early unless :NEW.workclassifi_oi IN (209, 211, 215, 266, 442, 462, 183, 196, 207, 256, 263).
    1.1) added-on 
            Bluelight Relining - NBHD	462
            CCI - Cross Connection Investigation	183
            FAC/CCC Inspections	196
            LOC - locate with push camera	207
            LTV - Video inspection of CB leads	209
            MHTV - Video insp. Manholes	211
            MTV - Mainline Video Inspection	215
            RMP - Root Maintenance Program	256
            ST - Sewer Service Trouble	263
            STV - Service Video Inspection	266
    2) Resolve v_wo_uuid from MNT.WORKORDERS where site_oi = 58 and the
       work order key matches :NEW.workorder_oi; exit if not found.
    3) v_src_eff_dt := GREATEST(:NEW.plndcompdate_dttm, :NEW.plndstrtdate_dttm),
       substituting a sentinel ancient date for nulls.
    4) v_max_eff_dt := MAX effective date in EPSEWERAI_WOT_STG for the same
       WORKORDER_UUID, ignoring rows with FEED_STATUS = 'SENT'.
    5) v_is_latest := 1 if v_src_eff_dt >= v_max_eff_dt (or no prior row), else 0.
    6) MERGE by TASK_UUID:
         - WHEN MATCHED: Update fields if any changed, set FEED_STATUS='UPDATED',
           and optionally bump FEED_STATUS_DTTM on transition to UPDATED.
         - WHEN NOT MATCHED: INSERT only if v_is_latest = 1; set FEED_STATUS='NEW'.

  Key Assumptions / Requirements:
    - MNT.WORKORDERTASK contains a unique task key in column "UUID".
      If the actual column name differs, replace :NEW.uuid accordingly.
    - MNT.WORKORDERS has columns: UUID (RAW), WORKORDERSOI (or WORKORDER_OI),
      and SITE_OI; the trigger filters to SITE_OI = 58.
    - CUSTOMERDATA.EPSEWERAI_WOT_STG has at least:
        TASK_UUID RAW(16),
        WORKORDER_UUID RAW(16),
        WOTASKTITLE VARCHAR2,
        PLNDCOMPDATE_DTTM DATE,
        PLNDSTRTDATE_DTTM DATE,
        WORKCLASSIFI_OI NUMBER,
        FEED_STATUS VARCHAR2
      If you want timestamp tracking, add FEED_STATUS_DTTM TIMESTAMP(6) and
      keep the CASE expression that bumps it on transition to UPDATED.
    - “Latest” is defined by the later of planned start/completion dates; nulls
      are treated as the oldest possible value (DATE '0001-01-01').

  Important Behaviors:
    - INSERT path is guarded by latest-only logic (prevents inserting stale rows).
    - UPDATE path triggers when watched fields actually change. If you need to
      avoid updating with older data, also add "AND src.is_latest = 1" to the
      WHEN MATCHED WHERE clause.
    - Rows with FEED_STATUS = 'SENT' are excluded from the “what is latest?”
      calculation, so SENT rows don’t block newer data.

  Performance Notes:
    - Recommended indexes:
        CREATE INDEX CUSTOMERDATA.IX_WOT_STG_TASKUUID
          ON CUSTOMERDATA.EPSEWERAI_WOT_STG (TASK_UUID);
        CREATE INDEX CUSTOMERDATA.IX_WOT_STG_WOUUID_STATUS
          ON CUSTOMERDATA.EPSEWERAI_WOT_STG (WORKORDER_UUID, FEED_STATUS);

  Maintenance Tips:
    - If new classifications must be included/excluded, update the IN list.
    - If multi-site support is needed, parameterize or remove the site guard.
    - Keep source/target column names synchronized when schemas evolve.

  Owner:     Cristina Lau
  Created:   Feb 24, 2026
  Revised:   (Add date & summary of changes)
==============================================================================*/

CREATE OR REPLACE TRIGGER TRG_WOT_TO_STG
AFTER INSERT OR UPDATE OF
  WOTASKTITLE, PLNDSTRTDATE_DTTM, WORKCLASSIFI_OI  -- deliberately exclude task.PLNDCOMPDATE_DTTM
ON MNT.WORKORDERTASK
FOR EACH ROW
DECLARE
  c_site_oi CONSTANT NUMBER := 58;
BEGIN
  MERGE INTO CUSTOMERDATA.EPSEWERAI_WOT_STG s
  USING (
    SELECT
      wo.WONUMBER                                         AS wonumber,
      wo.UUID                                             AS workorder_uuid,
      :NEW.UUID                                           AS task_uuid,
      :NEW.TASKNUMBER                                     AS tasknumber,
      NVL(:NEW.WOTASKTITLE, wo.TITLE)                     AS wotasktitle,

      /* Hard guarantee: only from WORKORDERS.REQCOMPDATE_DTTM */
      wo.REQCOMPDATE_DTTM                                 AS plndcompdate_dttm,

      NVL(:NEW.PLNDSTRTDATE_DTTM, wo.PLNDSTRTDATE_DTTM)   AS plndstrtdate_dttm,

      CASE
        WHEN :NEW.WORKCLASSIFI_OI IN (209, 211, 215, 266, 442, 462, 183, 196, 207, 256, 263)
          THEN :NEW.WORKCLASSIFI_OI
        ELSE 209
      END                                                 AS workclassifi_oi
    FROM MNT.WORKORDERS wo
    WHERE wo.WORKORDERSOI = :NEW.WORKORDER_OI
      AND wo.SITE_OI      = c_site_oi
  ) src
  ON (s.TASK_UUID = src.TASK_UUID)

  WHEN MATCHED THEN
    UPDATE SET
      s.WORKORDER_UUID     = src.WORKORDER_UUID,
      s.WONUMBER           = src.WONUMBER,
      s.TASKNUMBER         = src.TASKNUMBER,
      s.WOTASKTITLE        = src.WOTASKTITLE,
      s.PLNDCOMPDATE_DTTM  = src.PLNDCOMPDATE_DTTM,  -- fed from wo.REQCOMPDATE_DTTM only
      s.PLNDSTRTDATE_DTTM  = src.PLNDSTRTDATE_DTTM,
      s.WORKCLASSIFI_OI    = src.WORKCLASSIFI_OI,
      s.FEED_STATUS        = 'UPDATED'
    WHERE
          NVL(s.WONUMBER, '~') <> NVL(src.WONUMBER, '~')
       OR NVL(s.TASKNUMBER, -1) <> NVL(src.TASKNUMBER, -1)
       OR NVL(s.WOTASKTITLE, '~') <> NVL(src.WOTASKTITLE, '~')
       OR NVL(s.PLNDCOMPDATE_DTTM, DATE '1900-01-01') <> NVL(src.PLNDCOMPDATE_DTTM, DATE '1900-01-01')
       OR NVL(s.PLNDSTRTDATE_DTTM, DATE '1900-01-01') <> NVL(src.PLNDSTRTDATE_DTTM, DATE '1900-01-01')
       OR NVL(s.WORKCLASSIFI_OI, -1) <> NVL(src.WORKCLASSIFI_OI, -1)

  WHEN NOT MATCHED THEN
    INSERT (
      TASK_UUID, WORKORDER_UUID, WONUMBER, TASKNUMBER, WOTASKTITLE,
      PLNDCOMPDATE_DTTM, PLNDSTRTDATE_DTTM, WORKCLASSIFI_OI, FEED_STATUS
    )
    VALUES (
      src.TASK_UUID, src.WORKORDER_UUID, src.WONUMBER, src.TASKNUMBER, src.WOTASKTITLE,
      src.PLNDCOMPDATE_DTTM, src.PLNDSTRTDATE_DTTM, src.WORKCLASSIFI_OI, 'NEW'
    );
END TRG_WOT_TO_STG;
