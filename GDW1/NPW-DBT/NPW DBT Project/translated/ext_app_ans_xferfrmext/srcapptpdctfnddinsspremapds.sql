{{ config(materialized='view', tags=['Ext_APP_ANS_XFERFrmExt']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_cse__xs__chl__cpl__bus__app__ans__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_cse__xs__chl__cpl__bus__app__ans__premap")  }})
SrcApptPdctFnddInssPremapDS AS (
	SELECT HL_APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		CBA_STAFF_NUMBER,
		LODGEMENT_BRANCH_ID,
		SUBTYPE_CODE,
		MOD_TIMESTAMP,
		ORIG_ETL_D
	FROM _cba__app_lpxs_lpxs__dev_dataset_cse__xs__chl__cpl__bus__app__ans__premap
)

SELECT * FROM SrcApptPdctFnddInssPremapDS