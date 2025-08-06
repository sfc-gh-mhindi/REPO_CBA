{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_AMTFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app______var__dbt__dlta__vers______appt__pdct__amt AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app______var__dbt__dlta__vers______appt__pdct__amt")  }})
SrcApptPdctAmtDS AS (
	SELECT APPT_PDCT_I,
		AMT_TYPE_C,
		EFFT_D,
		EXPY_D,
		CNCY_C,
		APPT_PDCT_A,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM,
		DLTA_VERS
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app______var__dbt__dlta__vers______appt__pdct__amt
)

SELECT * FROM SrcApptPdctAmtDS