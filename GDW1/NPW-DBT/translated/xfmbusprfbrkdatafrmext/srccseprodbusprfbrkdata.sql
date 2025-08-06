{{ config(materialized='view', tags=['XfmBusPrfBrkDataFrmExt']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__prd__bus__prf__brk__data__cse__prd__bus__prf__brk__data__20100920 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__prd__bus__prf__brk__data__cse__prd__bus__prf__brk__data__20100920")  }})
SrcCseProdBusPrfBrkData AS (
	SELECT REC_TYPE,
		ALIAS_ID,
		GRADE,
		SUBGRADE,
		PRINT_ANYWHERE_FLAG
	FROM _cba__app_csel4_dev_inprocess_cse__prd__bus__prf__brk__data__cse__prd__bus__prf__brk__data__20100920
)

SELECT * FROM SrcCseProdBusPrfBrkData