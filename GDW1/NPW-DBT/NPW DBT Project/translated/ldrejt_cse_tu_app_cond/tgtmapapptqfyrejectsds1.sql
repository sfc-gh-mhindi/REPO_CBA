{{ config(materialized='view', tags=['LdREJT_CSE_TU_APP_COND']) }}

WITH 
_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__mapping__rejects__aqc AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__mapping__rejects__aqc")  }})
TgtMapApptQfyRejectsDS1 AS (
	SELECT SBTY_CODE,
		HL_APP_PROD_ID,
		TU_APP_CONDITION_ID,
		COND_APPT_CAT_ID,
		CONDITION_MET_DATE,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_hlt_dev_dataset_cse__chl__bus__tu__app__mapping__rejects__aqc
)

SELECT * FROM TgtMapApptQfyRejectsDS1