{{ config(materialized='view', tags=['XfmFunnelExt']) }}

WITH 
_cba__app_lpxs_lpxs__sit_dataset_cse__clp__bus__app__prod__cse__clp__bus__appt__qstn__20080215 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__sit_dataset_cse__clp__bus__app__prod__cse__clp__bus__appt__qstn__20080215")  }})
CLP_BUS_APP_PROD AS (
	SELECT APP_ID,
		APP_PROD_ID,
		SUBTYPE_CODE,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE
	FROM _cba__app_lpxs_lpxs__sit_dataset_cse__clp__bus__app__prod__cse__clp__bus__appt__qstn__20080215
)

SELECT * FROM CLP_BUS_APP_PROD