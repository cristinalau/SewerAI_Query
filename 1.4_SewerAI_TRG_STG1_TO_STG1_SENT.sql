CREATE OR REPLACE TRIGGER TRG_STG1_SENT_SYNC
FOR UPDATE OF FEED_STATUS ON CUSTOMERDATA.EPSEWERAI_WOT_STG1
COMPOUND TRIGGER

  -- Distinct TASK_UUIDs (WO UUIDs) that turned to SEND/SENT in this statement
  TYPE t_uuid_set IS TABLE OF PLS_INTEGER INDEX BY VARCHAR2(64);
  g_to_delete t_uuid_set;

  PROCEDURE add_uuid(p_uuid RAW) IS
  BEGIN
    IF p_uuid IS NOT NULL THEN
      g_to_delete(RAWTOHEX(p_uuid)) := 1; -- set semantics
    END IF;
  END;

  BEFORE STATEMENT IS
  BEGIN
    g_to_delete.DELETE;
  END BEFORE STATEMENT;

  AFTER EACH ROW IS
    v_old VARCHAR2(50);
    v_new VARCHAR2(50);
  BEGIN
    -- Normalize (case/whitespace) to make comparisons robust
    v_old := TRIM(UPPER(:OLD.FEED_STATUS));
    v_new := TRIM(UPPER(:NEW.FEED_STATUS));

    -- If status transitioned to SEND/SENT (from anything else), queue it
    IF v_new IN ('SEND','SENT')
       AND ( :OLD.FEED_STATUS IS NULL OR v_old NOT IN ('SEND','SENT') ) THEN

      -- 1) Push change down to STG rows (WORKORDER_UUID = STG1.TASK_UUID)
      UPDATE CUSTOMERDATA.EPSEWERAI_WOT_STG s
         SET s.FEED_STATUS = 'SENT'
       WHERE s.WORKORDER_UUID = :NEW.TASK_UUID;

      -- 2) Queue STG1 row for deletion after the statement
      add_uuid(:NEW.TASK_UUID);
    END IF;
  END AFTER EACH ROW;

  AFTER STATEMENT IS
    v_key VARCHAR2(64);
  BEGIN
    v_key := g_to_delete.FIRST;
    WHILE v_key IS NOT NULL LOOP
      -- Delete the STG1 row unconditionally for the queued UUID.
      -- (We purposely do NOT check FEED_STATUS again here.)
      DELETE FROM CUSTOMERDATA.EPSEWERAI_WOT_STG1
       WHERE TASK_UUID = HEXTORAW(v_key);
      v_key := g_to_delete.NEXT(v_key);
    END LOOP;
  END AFTER STATEMENT;

END TRG_STG1_SENT_SYNC;
