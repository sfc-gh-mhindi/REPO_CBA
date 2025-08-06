{{ config(materialized='view', tags=['LdREJT_XS_APP_ANS']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_cse__xs__chl__cpl__bus__app__ans__map__rej__xs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_cse__xs__chl__cpl__bus__app__ans__map__rej__xs")  }})
SrcAppCclAppRejectsDS AS (
	SELECT APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		CBA_STAFF_NUMBER,
		LODGEMENT_BRANCH_ID,
		SBTY_CODE,
		MOD_TIMESTAMP,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_lpxs_lpxs__dev_dataset_cse__xs__chl__cpl__bus__app__ans__map__rej__xs
)

SELECT * FROM SrcAppCclAppRejectsDS