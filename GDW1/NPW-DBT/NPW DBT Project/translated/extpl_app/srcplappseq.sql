{{ config(materialized='view', tags=['ExtPL_APP']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__app__cse__cpl__bus__app__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__app__cse__cpl__bus__app__20060616")  }})
SrcPlAppSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__app__cse__cpl__bus__app__20060616
)

SELECT * FROM SrcPlAppSeq