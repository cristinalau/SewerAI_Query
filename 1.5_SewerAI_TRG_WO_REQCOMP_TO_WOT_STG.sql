/*==============================================================================
  Trigger:   TRG_WO_REQCOMP_TO_WOT_STG
  Scope:     MNT.WORKORDERS (row-level)
  Timing:    AFTER UPDATE OF REQCOMPDATE_DTTM
  Purpose:   When the parent Work Order REQCOMPDATE_DTTM changes, push the new
             value into staging (PLNDCOMPDATE_DTTM) for all related tasks, and
             set FEED_STATUS='UPDATED' only when the value actually changes.
==============================================================================*/
CREATE OR REPLACE TRIGGER TRG_WO_REQCOMP_TO_WOT_STG
AFTER UPDATE OF REQCOMPDATE_DTTM ON MNT.WORKORDERS
FOR EACH ROW
WHEN (NEW.SITE_OI = 58)  -- keep same site guard
DECLARE
BEGIN
  /* Update all staging rows for tasks under this work order UUID.
     We compare NULL-safe to avoid unnecessary updates. */
  UPDATE CUSTOMERDATA.EPSEWERAI_WOT_STG s
     SET s.PLNDCOMPDATE_DTTM = :NEW.REQCOMPDATE_DTTM,
         s.FEED_STATUS       = 'UPDATED'
   WHERE s.WORKORDER_UUID = :NEW.UUID
     -- OPTIONAL: un-comment to avoid touching rows that are SENT
     -- AND s.FEED_STATUS <> 'SENT'
     AND NVL(s.PLNDCOMPDATE_DTTM, DATE '1900-01-01')
         <> NVL(:NEW.REQCOMPDATE_DTTM, DATE '1900-01-01');
END TRG_WO_REQCOMP_TO_WOT_STG;
