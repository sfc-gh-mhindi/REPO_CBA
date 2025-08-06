{{ config(materialized='view', tags=['LdUtilAcctIdnnErorUpdCommon']) }}

WITH 
_cba__app_csel4_dev_dataset_util__acct__idnn__eror__u__cse__com__cpo__bus__ncpr__clnt__20101020 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_util__acct__idnn__eror__u__cse__com__cpo__bus__ncpr__clnt__20101020")  }})
UTIL_ACCT_IDNN_EROR_U AS (
	SELECT EROR_ACCT_NUMB,
		PROS_KEY_EFFT_I,
		EXPY_DATE,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_util__acct__idnn__eror__u__cse__com__cpo__bus__ncpr__clnt__20101020
)

SELECT * FROM UTIL_ACCT_IDNN_EROR_U