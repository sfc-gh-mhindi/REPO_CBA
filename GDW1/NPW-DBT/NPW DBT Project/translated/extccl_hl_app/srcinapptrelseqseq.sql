{{ config(materialized='view', tags=['ExtCCL_HL_APP']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__ccl__bus__hl__app__cse__ccl__bus__hl__app__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__ccl__bus__hl__app__cse__ccl__bus__hl__app__20060616")  }})
SrcInApptRelSeqSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		CCL_HL_APP_ID,
		CCL_APP_ID,
		HL_APP_ID,
		LMI_AMT,
		HL_PACKAGE_CAT_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__ccl__bus__hl__app__cse__ccl__bus__hl__app__20060616
)

SELECT * FROM SrcInApptRelSeqSeq