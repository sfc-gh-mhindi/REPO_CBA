{{ config(materialized='view', tags=['LdREJT_CSE_TU_APP']) }}

WITH 
_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__mapping__rejects1 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__mapping__rejects1")  }})
TgtMapApptQfyRejectsDS1 AS (
	SELECT SUBTYPE_CODE,
		HL_APP_PROD_ID,
		TU_APP_ID,
		TU_ACCOUNT_ID,
		TU_DOCCOLLECT_METHOD_CAT_ID,
		DOCCOLLECT_ADDRESS_TYPE_ID,
		DOCCOLLECT_BSB,
		TU_DOCCOLLECT_ADDRESS_LINE_1,
		TU_DOCCOLLECT_ADDRESS_LINE_2,
		TU_DOCCOLLECT_SUBURB,
		TU_DOCCOLLECT_STATE_ID,
		TU_DOCCOLLECT_POSTCODE,
		TU_DOCCOLLECT_COUNTRY_ID,
		TU_DOCCOLLECT_OVERSEA_STATE,
		TOPUP_AMOUNT,
		TOPUP_AGENT_ID,
		TOPUP_AGENT_NAME,
		ACCOUNT_NO,
		CRIS_PRODUCT_ID,
		ORIG_ETL_D,
		ETL_D,
		EROR_C
	FROM _cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__mapping__rejects1
)

SELECT * FROM TgtMapApptQfyRejectsDS1