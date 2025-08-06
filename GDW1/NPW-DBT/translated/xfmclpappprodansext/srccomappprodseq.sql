{{ config(materialized='view', tags=['XfmClpAppProdAnsExt']) }}

WITH 
_cba__app_lpxs_lpxs__dev_inprocess_cse__clp__bus__app__prod__ans__cse__clp__bus__appt__qstn__20080114 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_inprocess_cse__clp__bus__app__prod__ans__cse__clp__bus__appt__qstn__20080114")  }})
SrcComAppProdSeq AS (
	SELECT APP_RECORD_TYPE,
		MOD_TIMESTAMP,
		APP_ID,
		APP_PROD_ID,
		SUBTYPE_CODE,
		CLP_APP_PROD_ANSWER_VN,
		CLP_APP_PROD_ANSWER_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		YN_FLAG_ANSWER,
		NUMERIC_ANSWER,
		DATE_ANSWER,
		MOD_USER_ID,
		DUMMY
	FROM _cba__app_lpxs_lpxs__dev_inprocess_cse__clp__bus__app__prod__ans__cse__clp__bus__appt__qstn__20080114
)

SELECT * FROM SrcComAppProdSeq