{{ config(materialized='view', tags=['XfmHL_APPFrmExt']) }}

WITH 
_cba__app_pj__rapidresponseteam_csel4_dev_dataset_cse__chl__bus__app__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__rapidresponseteam_csel4_dev_dataset_cse__chl__bus__app__premap")  }})
SrcHlAppPremapDS AS (
	SELECT HL_APP_ID,
		HL_APP_PROD_ID,
		HL_PACKAGE_CAT_ID,
		LPC_OFFICE,
		STATUS_TRACKER_ID,
		CHL_APP_PCD_EXT_SYS_CAT_ID,
		CHL_APP_SIMPLE_APP_FLAG,
		CHL_APP_ORIGINATING_AGENT_ID,
		CHL_APP_AGENT_NAME,
		CASS_WITHHOLD_RISKBANK_FLAG,
		CR_DATE,
		ASSESSMENT_DATE,
		NCPR_FLAG,
		CAMPAIGN_CODE,
		PEXA_FLAG,
		HSCA_FLAG,
		HSCA_CONVERTED_TO_FULL_AT,
		ORIG_ETL_D
	FROM _cba__app_pj__rapidresponseteam_csel4_dev_dataset_cse__chl__bus__app__premap
)

SELECT * FROM SrcHlAppPremapDS