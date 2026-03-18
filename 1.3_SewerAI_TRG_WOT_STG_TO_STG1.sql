--Insert to CUSTOMERDATA.EPSEWERAI_WOT_STG1 if new and updated workorder

CREATE OR REPLACE TRIGGER TRG_WOT_STG_TO_STG1
AFTER INSERT OR UPDATE OF feed_status, wotasktitle, plndcompdate_dttm, plndstrtdate_dttm, workclassifi_oi
ON CUSTOMERDATA.EPSEWERAI_WOT_STG
FOR EACH ROW
BEGIN
  -- Only act for NEW / UPDATED
  IF :NEW.feed_status IN ('NEW','UPDATED') THEN
    INSERT INTO CUSTOMERDATA.EPSEWERAI_WOT_STG1 (
      task_uuid,
      wotasktitle,
      plndcompdate_dttm,
      plndstrtdate_dttm,
      workclassifi_oi,
      feed_status
    )
    SELECT
      :NEW.workorder_uuid,      -- map WO UUID -> STG1.TASK_UUID
      :NEW.wotasktitle,
      :NEW.plndcompdate_dttm,
      :NEW.plndstrtdate_dttm,
      :NEW.workclassifi_oi,
      :NEW.feed_status
    FROM dual
    WHERE NOT EXISTS (
      SELECT 1
      FROM CUSTOMERDATA.EPSEWERAI_WOT_STG1 t
      WHERE t.task_uuid = :NEW.workorder_uuid
    );
  END IF;
END;
