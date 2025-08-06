{{ config(materialized='view', tags=['Ext_Fn_Hl_PL']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_lpxs__app__ans__pl__20071119 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_lpxs__app__ans__pl__20071119")  }})
Data_Set_PL AS (
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
	FROM _cba__app_lpxs_lpxs__dev_dataset_lpxs__app__ans__pl__20071119
)

SELECT * FROM Data_Set_PL