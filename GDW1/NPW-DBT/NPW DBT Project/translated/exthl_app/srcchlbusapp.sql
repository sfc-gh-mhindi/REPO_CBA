{{ config(materialized='view', tags=['ExtHL_APP']) }}

WITH 
_cba__app_csel4_sit1_inprocess_cse__chl__bus__app__cse__chl__bus__app__20141106 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit1_inprocess_cse__chl__bus__app__cse__chl__bus__app__20141106")  }})
SrcChlBusApp AS (
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
		FOREIGN_INCOME_FLAG,
		FI_CURRENCY_CODE,
		PEXA_FLAG,
		HSCA_FLAG,
		HSCA_CONVERTED_TO_FULL_AT,
		CHL_APP_DUMMY
	FROM _cba__app_csel4_sit1_inprocess_cse__chl__bus__app__cse__chl__bus__app__20141106
)

SELECT * FROM SrcChlBusApp