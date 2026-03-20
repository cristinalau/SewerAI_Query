SET DEFINE OFF;
SET SERVEROUTPUT ON;
WHENEVER SQLERROR EXIT SQL.SQLCODE;

PROMPT === 1.0 Create EPSEWERAI_WOT_STG ===
@1.0_SewerAI_Create_Table_EPSEWERAI_WOT_STG.sql

PROMPT === 1.1 Create EPSEWERAI_WOT_STG1 ===
@1.1_SewerAI_Create_Table_EPSEWERAI_WOT_STG1.sql

PROMPT === 1.2 Trigger TRG_WOT_TO_STG ===
@1.2_SewerAI_TRG_WOT_TO_STG.sql

PROMPT === 1.3 Trigger TRG_WOT_STG_TO_STG1 ===
@1.3_SewerAI_TRG_WOT_STG_TO_STG1.sql

PROMPT === 1.4 Trigger TRG_STG1_SENT_SYNC ===
@1.4_SewerAI_TRG_STG1_TO_STG1_SENT.sql

PROMPT === 1.5 Trigger TRG_WO_REQCOMP_TO_WOT_STG ===
@1.5_SewerAI_TRG_WO_REQCOMP_TO_WOT_STG.sql

PROMPT === 1.6 Procedure PRC_WOT_TO_STG_SYNC ===
@1.6_SewerAI_PRC_WOT_TO_STG_SYNC.sql

PROMPT === 1.7 Job JOB_WOT_TO_STG_SYNC ===
@1.7_SewerAI_JOB_WOT_TO_STG_SYNC.sql

PROMPT === 4 Shape lookup ===
@4_SewerAI_Test_Create_Shape_Code.sql

PROMPT === 5 Material lookup ===
@5_SewerAI_Test_Create_Material_Code.sql

PROMPT === 6 Wall/Bench/Channel lookup ===
@6_SewerAI_Test_Create_Wall_Bench_Channel_Code.sql

PROMPT === 2 Create SEWERAI_INSPECTIONS_V ===
@2_SewerAI_Create_View_SEWERAI_INSPECTIONS_V.sql

PROMPT === 3 Create EPSEWERAI_CR_INSPECT ===
@3_SewerAI_Create_Table_EPSEWERAI_CR_INSPECT.sql

PROMPT === 8 Trigger TRG_CR_INSPECT ===
@8_SewerAI_TRG_CR_INSPECT.sql

PROMPT === 9 Trigger TRG_CR_INSPECT_CLEAN_AI ===
@9_SewerAI_TRG_CR_INSPECT_CLEAN_AI.sql

PROMPT === 10.0 Trigger TRG_CR_INSP_NEW_TO_WOT_UPDATED ===
@10.0_SewerAI_TRG_CR_INSPECT_UPD_WOT_STG1.sql

PROMPT === 11 Procedure SEWERAI_SYNC_FEEDSTATUS ===
@11_SewerAI_SYNC_FEEDSTATUS.sql

PROMPT === 12 Job SEWERAI_SYNC_FEEDSTATUS_JOB ===
@12_SEWERAI_SYNC_FEEDSTATUS_JOB.sql

PROMPT === 7 Create V_CR_INSPECT_TO_SEND ===
@7_SewerAI_Create_View_V_CR_INSPECT_TO_SEND.sql

PROMPT === DONE (8.1 intentionally excluded) ===
