{{ config(materialized='view', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

WITH 
_cba__app_hlt_sit_dataset_cse__clp__bus__appt__qstn__premap__20080311 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_dataset_cse__clp__bus__appt__qstn__premap__20080311")  }})
SrcCCAppProdBalXferPremapDS AS (
	SELECT APP_ID,
		SUBTYPE_CODE,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE
	FROM _cba__app_hlt_sit_dataset_cse__clp__bus__appt__qstn__premap__20080311
)

SELECT * FROM SrcCCAppProdBalXferPremapDS