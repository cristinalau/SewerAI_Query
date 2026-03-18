-- Update the latest feed row per inspection to FEED_STATUS='UPDATED'
-- when either ASSET_NUMBER or ADDITIONAL_INFORMATION differs from the view (SEWERAI_INSPECTIONS_V),
-- but only if INSPECTION_SID is NOT NULL (otherwise we 'wait').
-- In the current procedure, simply updating INSPECTION_SID (e.g., from NULL ? non?NULL) does not cause FEED_STATUS to change.
-- The procedure only updates a row when either:
-- PO_NUMBER (feed) ? ASSET_NUMBER (view), or
-- ADDITIONAL_INFORMATION (feed) ? ADDITIONAL_INFORMATION (view)
-- It also requires INSPECTION_SID IS NOT NULL as a precondition (a filter), but it does not treat a change in INSPECTION_SID itself as a reason to update FEED_STATUS.
-- So, if INSPECTION_SID becomes non?NULL but the two compared fields are unchanged, no update happens and FEED_STATUS remains as-is.

CREATE OR REPLACE PROCEDURE CUSTOMERDATA.SEWERAI_SYNC_FEEDSTATUS AS
BEGIN
  MERGE INTO CUSTOMERDATA."EPSEWERAI_CR_INSPECT" tgt
  USING (
    WITH v AS (
      SELECT
        /* Canonicalize DR_UUID (from view) into dashed, lowercase to match feed INSPECTIONID */
        LOWER(
          REGEXP_REPLACE(
            REGEXP_REPLACE(NVL(v."DR_UUID", ''), '[^0-9A-Fa-f]', ''),
            '(^[0-9A-Fa-f]{8})([0-9A-Fa-f]{4})([0-9A-Fa-f]{4})([0-9A-Fa-f]{4})([0-9A-Fa-f]{12}$)',
            '\1-\2-\3-\4-\5'
          )
        )                                  AS inspectionid_can,
        v."ASSET_NUMBER"                   AS v_asset_number,
        v."ADDITIONAL_INFORMATION"         AS v_addl_info
      FROM CUSTOMERDATA."SEWERAI_INSPECTIONS_V" v
    ),

    latest_feed AS (
      /* Latest feed row per INSPECTIONID */
      SELECT *
      FROM (
        SELECT
          t."INSPECTIONID",
          t."INSPECTION_SID",
          t."PO_NUMBER",                 -- this is the stored ASSET_NUMBER
          t."ADDITIONAL_INFORMATION",
          t."LASTUPDATE_DTTM",
          t."CREATEDATE_DTTM",
          ROWID AS rid,
          ROW_NUMBER() OVER (
            PARTITION BY t."INSPECTIONID"
            ORDER BY NVL(t."LASTUPDATE_DTTM", t."CREATEDATE_DTTM") DESC, ROWID DESC
          ) AS rn
        FROM CUSTOMERDATA."EPSEWERAI_CR_INSPECT" t
      )
      WHERE rn = 1
    ),

    candidates AS (
      SELECT
        lf.rid,
        v.v_asset_number,
        v.v_addl_info
      FROM latest_feed lf
      JOIN v
        ON v.inspectionid_can = lf."INSPECTIONID"
      WHERE lf."INSPECTION_SID" IS NOT NULL
        AND (
             -- Detect change in ASSET_NUMBER (feed.PO_NUMBER vs view.ASSET_NUMBER)
             NVL(lf."PO_NUMBER", '¤') <> NVL(v.v_asset_number, '¤')

             OR

             -- Detect change in ADDITIONAL_INFORMATION (LOB-safe)
             CASE
               WHEN lf."ADDITIONAL_INFORMATION" IS NULL AND v.v_addl_info IS NULL THEN 0
               WHEN lf."ADDITIONAL_INFORMATION" IS NULL AND v.v_addl_info IS NOT NULL THEN 1
               WHEN lf."ADDITIONAL_INFORMATION" IS NOT NULL AND v.v_addl_info IS NULL THEN 1
               ELSE DBMS_LOB.COMPARE(lf."ADDITIONAL_INFORMATION", v.v_addl_info)
             END <> 0
        )
    )
    SELECT *
    FROM candidates
  ) src
  ON (tgt.ROWID = src.rid)
  WHEN MATCHED THEN
    UPDATE SET
      -- Sync the two fields into the feed row (optional but recommended for downstream parity)
      tgt."PO_NUMBER"                = src.v_asset_number,     -- store latest ASSET_NUMBER
      tgt."ADDITIONAL_INFORMATION"   = src.v_addl_info,
      tgt."FEED_STATUS"              = 'UPDATED',
      tgt."LASTUPDATE_DTTM"          = SYSDATE;

  COMMIT;
END;

