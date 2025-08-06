{{ config(materialized='view', tags=['LdAPPT_PDCT_FNDD_INSSUpd']) }}

WITH 
_cba__app_hlt_dev_dataset_appt__pdct__fndd__inss__u__cse__chl__bus__tu__app__fund__det__20071105 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_appt__pdct__fndd__inss__u__cse__chl__bus__tu__app__fund__det__20071105")  }})
TgtAPPT_PDCT_DOCU_DELY_INSSUpDS AS (
	SELECT APPT_PDCT_I,
		APPT_PDCT_FNDD_I,
		APPT_PDCT_FNDD_METH_I,
		FNDD_INSS_METH_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_hlt_dev_dataset_appt__pdct__fndd__inss__u__cse__chl__bus__tu__app__fund__det__20071105
)

SELECT * FROM TgtAPPT_PDCT_DOCU_DELY_INSSUpDS