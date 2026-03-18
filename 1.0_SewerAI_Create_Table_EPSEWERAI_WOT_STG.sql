------------------------------------------------------------
-- Step 1: Drop staging table if exists (CUSTOMERDATA)
------------------------------------------------------------

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE CUSTOMERDATA.EPSEWERAI_WOT_STG';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- expected if first run
END;
/

------------------------------------------------------------
-- Step 2: Create staging table
------------------------------------------------------------
-- DROP TABLE CUSTOMERDATA.EPSEWERAI_WOT_STG PURGE;


CREATE TABLE "CUSTOMERDATA"."EPSEWERAI_WOT_STG" 
   (	"TASK_UUID" RAW(16) DEFAULT SYS_GUID() NOT NULL ENABLE, 
	"WORKORDER_UUID" RAW(16), 
	"WOTASKTITLE" VARCHAR2(503 BYTE), 
	"PLNDCOMPDATE_DTTM" DATE, 
	"PLNDSTRTDATE_DTTM" DATE, 
	"WORKCLASSIFI_OI" NUMBER, 
	"FEED_STATUS" VARCHAR2(50 BYTE), 
	"FEED_STATUS_DTTM" TIMESTAMP (6) WITH TIME ZONE
   ) 

COMMENT ON COLUMN "CUSTOMERDATA"."EPSEWERAI_WOT_STG"."FEED_STATUS_DTTM" IS 'Timestamp when FEED_STATUS was last set/changed by trigger';
