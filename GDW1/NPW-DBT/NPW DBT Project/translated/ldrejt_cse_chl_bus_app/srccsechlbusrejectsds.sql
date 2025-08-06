{{ config(materialized='view', tags=['LdREJT_CSE_CHL_BUS_APP']) }}

WITH 
_cba__app_mme_dev_dataset_cse__chl__bus__app__mapping__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_mme_dev_dataset_cse__chl__bus__app__mapping__rejects")  }})
SrcCseChlBusRejectsDS AS (
	SELECT HL_APP_ID,
		HL_PACKAGE_CAT_ID,
		LPC_OFFICE,
		STATUS_TRACKER_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C,
		HL_APP_PROD_ID,
		CHL_APP_PCD_EXT_SYS_CAT_ID,
		CHL_APP_SIMPLE_APP_FLAG,
		CHL_APP_ORIGINATING_AGENT_ID,
		CHL_APP_AGENT_NAME,
		CASS_WITHHOLD_RISKBANK_FLAG,
		CR_DATE,
		ASSESSMENT_DATE,
		NCPR_FLAG,
		FHB_FLAG,
		SETTLEMENT_DATE,
		PEXA_FLAG,
		HSCA_FLAG,
		HSCA_CONVERTED_TO_FULL_AT
	FROM _cba__app_mme_dev_dataset_cse__chl__bus__app__mapping__rejects
)

SELECT * FROM SrcCseChlBusRejectsDS