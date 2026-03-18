-- Optional: DROP TRIGGER TRG_CR_INSPECT;

-- Trigger to pick the workorder to send to SewerAI
CREATE OR REPLACE TRIGGER TRG_CR_INSPECT
FOR INSERT ON CUSTOMERDATA.EPDRFACWORKHISTORY
COMPOUND TRIGGER

  TYPE t_keyset IS TABLE OF PLS_INTEGER INDEX BY VARCHAR2(64);
  g_keys t_keyset;

  BEFORE STATEMENT IS
  BEGIN
    g_keys.DELETE;
  END BEFORE STATEMENT;

  AFTER EACH ROW IS
  BEGIN
    IF :NEW.EPDRFACILITYWORKHISTORYOI IS NOT NULL THEN
      g_keys(TO_CHAR(:NEW.EPDRFACILITYWORKHISTORYOI)) := 1;
    END IF;
  END AFTER EACH ROW;

  AFTER STATEMENT IS
    l_k VARCHAR2(64);
  BEGIN
    l_k := g_keys.FIRST;
    WHILE l_k IS NOT NULL LOOP

      INSERT INTO CUSTOMERDATA."EPSEWERAI_CR_INSPECT" (
        "PROJECT_SID",
        "INSPECTION_TYPE",
        "INSPECTIONID",
        "WORK_ORDER_UUID",
        "WORKORDER",
        "PROJECT",
        "PO_NUMBER",
        "ADDITIONAL_INFORMATION",
        "PIPE_SEGMENT_REFERENCE",
        "LATERAL_SEGMENT_REFERENCE",
        "MANHOLE_NUMBER",
        "MATERIAL",
        "PIPE_USE",
        "COVER_SHAPE",
        "UPSTREAM_MH",
        "DOWNSTREAM_MH",
        "FACILITY_TYPE",
        "FACILITY_ID",
        "FACILITYOI",
        "PIP_TYPE",
        "SHAPE",
        "ACCESS_TYPE",
        "MH_USE",
        "WALL_MATERIAL",
        "BENCH_MATERIAL",
        "CHANNEL_MATERIAL",
        "WALL_BYSIZE",
        "WALL_DEPTH",
        "ELEVATION",
        "FRAME_MATERIAL",
        "HEIGHT",
        "UP_ELEVATION",
        "UP_GRADE_TO_INVERT",
        "DOWN_ELEVATION",
        "DOWN_GRADE_TO_INVERT",
        "STREET",
        "TOTAL_LENGTH",
        "YEAR_CONSTRUCTED",
        "SIZE",
        "DRAINAGE_AREA",
        "UNKNOWN_TYPE",
        "CREATEDATE_DTTM",
        "LASTUPDATE_DTTM",
        "FEED_STATUS"
      )
      SELECT
        /* PROJECT_SID (from WORK_ORDER_TASK_UUID) -> canonical dashed, lowercase */
        LOWER(
          REGEXP_REPLACE(
            REGEXP_REPLACE(NVL(v."WORK_ORDER_TASK_UUID", ''), '[^0-9A-Fa-f]', ''),
            '(^[0-9A-Fa-f]{8})([0-9A-Fa-f]{4})([0-9A-Fa-f]{4})([0-9A-Fa-f]{4})([0-9A-Fa-f]{12}$)',
            '\1-\2-\3-\4-\5'
          )
        ) AS "PROJECT_SID",

        v."INSPECTION_TYPE",

        /* INSPECTIONID (from DR_UUID) -> canonical dashed, lowercase */
        LOWER(
          REGEXP_REPLACE(
            REGEXP_REPLACE(NVL(v."DR_UUID", ''), '[^0-9A-Fa-f]', ''),
            '(^[0-9A-Fa-f]{8})([0-9A-Fa-f]{4})([0-9A-Fa-f]{4})([0-9A-Fa-f]{4})([0-9A-Fa-f]{12}$)',
            '\1-\2-\3-\4-\5'
          )
        ) AS "INSPECTIONID",

        /* WORK_ORDER_UUID -> canonical dashed, lowercase */
        LOWER(
          REGEXP_REPLACE(
            REGEXP_REPLACE(NVL(v."WORK_ORDER_UUID", ''), '[^0-9A-Fa-f]', ''),
            '(^[0-9A-Fa-f]{8})([0-9A-Fa-f]{4})([0-9A-Fa-f]{4})([0-9A-Fa-f]{4})([0-9A-Fa-f]{12}$)',
            '\1-\2-\3-\4-\5'
          )
        ) AS "WORK_ORDER_UUID",

        v."WORK_ORDERS_NUMBER",
        v."WORK_ORDER_TASK_TITLE",
        v."ASSET_NUMBER",
        v."ADDITIONAL_INFORMATION",
        v."PIPE_SEGMENT_REFERENCE",
        v."LATERAL_SEGMENT_REFERENCE",
        v."MANHOLE_NUMBER",
        v."MATERIAL",
        v."PIPE_USE",
        v."COVER_SHAPE",
        v."UPSTREAM_MH",
        v."DOWNSTREAM_MH",
        v."FACILITY_TYPE",
        v."FACILITY_ID",
        v."FACILITYOI",
        v."PIP_TYPE",
        v."SHAPE",
        v."ACCESS_TYPE",
        v."MH_USE",
        v."WALL_MATERIAL",
        v."BENCH_MATERIAL",
        v."CHANNEL_MATERIAL",
        v."WALL_BYSIZE",
        v."WALL_DEPTH",
        v."ELEVATION",
        v."FRAME_MATERIAL",
        v."HEIGHT",
        v."UP_ELEVATION",
        v."UP_GRADE_TO_INVERT",
        v."DOWN_ELEVATION",
        v."DOWN_GRADE_TO_INVERT",
        v."STREET",
        v."TOTAL_LENGTH",
        v."YEAR_CONSTRUCTED",
        v."SIZE",
        v."DRAINAGE_AREA",
        v."UNKNOWN_TYPE",
        v."CREATEDATE_DTTM",
        v."LASTUPDATE_DTTM",
        'NEW'
      FROM CUSTOMERDATA."SEWERAI_INSPECTIONS_V" v
      WHERE v."EPDRFACILITYWORKHISTORYOI" = TO_NUMBER(l_k)
        AND NOT EXISTS (
          SELECT 1
          FROM CUSTOMERDATA."EPSEWERAI_CR_INSPECT" t
          WHERE t."INSPECTIONID" = v."DR_UUID"
        );

      l_k := g_keys.NEXT(l_k);
    END LOOP;
  END AFTER STATEMENT;

END;