{{ config(materialized='view', tags=['Ld_APPT_PDCT_COND_Ins']) }}

WITH 
_cba__app_hlt_dev_dataset_appt__pdct__cond__i__cse__chl__bus__tu__app__cond__20071024 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_appt__pdct__cond__i__cse__chl__bus__tu__app__cond__20071024")  }})
TgtAPPT_PDCT_CONDInsertDS AS (
	SELECT APPT_PDCT_I,
		COND_C,
		EFFT_D,
		EXPY_D,
		APPT_PDCT_COND_MEET_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_hlt_dev_dataset_appt__pdct__cond__i__cse__chl__bus__tu__app__cond__20071024
)

SELECT * FROM TgtAPPT_PDCT_CONDInsertDS