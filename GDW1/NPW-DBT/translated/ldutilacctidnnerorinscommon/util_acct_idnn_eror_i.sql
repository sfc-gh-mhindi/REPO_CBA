{{ config(materialized='view', tags=['LdUtilAcctIdnnErorInsCommon']) }}

WITH 
_cba__app_csel4_dev_dataset_util__acct__idnn__eror__i__cse__com__cpo__bus__ncpr__clnt__20101020 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_util__acct__idnn__eror__i__cse__com__cpo__bus__ncpr__clnt__20101020")  }})
UTIL_ACCT_IDNN_EROR_I AS (
	SELECT EROR_ACCT_NUMB,
		CONV_M,
		EROR_TABL_NAME,
		EROR_COLM_NAME,
		RJCT_REAS,
		LOAD_S,
		PROS_KEY_EFFT_I,
		EXPY_DATE,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C,
		RJCT_RECD
	FROM _cba__app_csel4_dev_dataset_util__acct__idnn__eror__i__cse__com__cpo__bus__ncpr__clnt__20101020
)

SELECT * FROM UTIL_ACCT_IDNN_EROR_I