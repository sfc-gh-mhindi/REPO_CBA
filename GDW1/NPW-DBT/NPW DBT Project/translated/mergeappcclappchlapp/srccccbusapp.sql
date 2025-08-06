{{ config(materialized='view', tags=['MergeAppCclAppChlApp']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__ccc__bus__app__cse__com__bus__ccl__chl__com__app__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__ccc__bus__app__cse__com__bus__ccl__chl__com__app__20100614")  }})
SrcCccBusApp AS (
	SELECT CCC_APP_RECORD_TYPE,
		CCC_APP_MOD_TIMESTAMP,
		CCC_APP_CC_APP_ID,
		CCC_APP_CC_APP_CAT_ID
	FROM _cba__app_csel4_dev_inprocess_cse__ccc__bus__app__cse__com__bus__ccl__chl__com__app__20100614
)

SELECT * FROM SrcCccBusApp