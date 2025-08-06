{{ config(materialized='view', tags=['ExtAppCon_TuApp']) }}

WITH 
_cba__app_hlt_dev_inprocess_cse__chl__bus__tu__app__cond__cse__chl__bus__tu__app__cond__20071016 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_inprocess_cse__chl__bus__tu__app__cond__cse__chl__bus__tu__app__cond__20071016")  }})
SrcInApptRelSeqSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		TU_APP_CONDITION_ID,
		TU_APP_ID,
		TU_APP_CONDITION_CAT_ID,
		CONDITION_MET_FLAG,
		CONDITION_MET_DATE,
		MOD_USER_ID,
		SM_CASE_STATE_ID,
		SUBTYPE_CODE,
		HL_APP_PROD_ID,
		DUMMY
	FROM _cba__app_hlt_dev_inprocess_cse__chl__bus__tu__app__cond__cse__chl__bus__tu__app__cond__20071016
)

SELECT * FROM SrcInApptRelSeqSeq