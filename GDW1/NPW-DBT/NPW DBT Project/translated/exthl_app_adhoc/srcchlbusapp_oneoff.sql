{{ config(materialized='view', tags=['ExtHL_APP_Adhoc']) }}

WITH 
_cba__app_pj__rapidresponseteam_csel4_dev_inprocess_one__off__load__pexa__bus__app__cse__chl__bus__app__20151001 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__rapidresponseteam_csel4_dev_inprocess_one__off__load__pexa__bus__app__cse__chl__bus__app__20151001")  }})
SrcChlBusApp_oneoff AS (
	SELECT CHL_APP_RECORD_TYPE,
		CHL_APP_MOD_TIMESTAMP,
		CHL_APP_HL_APP_ID,
		HL_APP_PROD_ID,
		CHL_APP_HL_PACKAGE_CAT_ID,
		CHL_APP_LPC_OFFICE,
		CHL_APP_STATUS_TRACKER_ID,
		CHL_APP_PCD_EXTERNAL_SYS_CAT_ID,
		CHL_APP_SIMPLE_APP_FLAG,
		CHL_APP_ORIGINATING_AGENT_ID,
		CHL_APP_AGENT_NAME,
		CASS_WITHHOLD_RISKBANK_FLAG,
		REPRINT_COUNT,
		EXEC_DOCUMENTS_RECEIVER_TYPE,
		CR_DATE,
		ASSESSMENT_DATE,
		NCPR_FLAG,
		CAMPAIGN_CODE,
		FHB_FLAG,
		SETTLEMENT_DATE,
		SEFI_FLAG,
		PEXA_FLAG,
		HSCA_FLAG,
		HSCA_CONVERTED_TO_FULL_AT,
		CHL_APP_DUMMY
	FROM _cba__app_pj__rapidresponseteam_csel4_dev_inprocess_one__off__load__pexa__bus__app__cse__chl__bus__app__20151001
)

SELECT * FROM SrcChlBusApp_oneoff