{{ config(materialized='view', tags=['Ext_Pl_File_Ds']) }}

WITH 
_cba__app_lpxs_lpxs__dev_inprocess_cse__cpl__bus__pl__app__ans__cpl__bus__pl__app__ans__20071119 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_inprocess_cse__cpl__bus__pl__app__ans__cpl__bus__pl__app__ans__20071119")  }})
Sequential_File_0 AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		HL_APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		CBA_STAFF_NUMBER,
		LODGEMENT_BRANCH_ID,
		SUBTYPE_CODE,
		Dummy
	FROM _cba__app_lpxs_lpxs__dev_inprocess_cse__cpl__bus__pl__app__ans__cpl__bus__pl__app__ans__20071119
)

SELECT * FROM Sequential_File_0