{{ config(materialized='view', tags=['LdAPPT_PDCT_DOCU_DELY_INSSUpd']) }}

WITH 
_cba__app_hlt_dev_dataset_appt__pdct__docu__dely__inss__u__cse__chl__bus__tu__app__20071024 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_appt__pdct__docu__dely__inss__u__cse__chl__bus__tu__app__20071024")  }})
TgtAPPT_PDCT_DOCU_DELY_INSSUpDS AS (
	SELECT APPT_PDCT_I,
		DOCU_DELY_METH_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_hlt_dev_dataset_appt__pdct__docu__dely__inss__u__cse__chl__bus__tu__app__20071024
)

SELECT * FROM TgtAPPT_PDCT_DOCU_DELY_INSSUpDS