{{ config(materialized='view', tags=['DltMAP_CSE_SM_CASEFrmMAP_CSE_SM_CASE_DS']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__com__bus__ccl__chl__com__app__map__cse__sm__case AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__com__bus__ccl__chl__com__app__map__cse__sm__case")  }})
Tgt_MapCseSmCaseDS AS (
	SELECT SM_CASE_ID,
		TARG_I,
		TARG_SUBJ
	FROM _cba__app_csel4_csel4dev_dataset_cse__com__bus__ccl__chl__com__app__map__cse__sm__case
)

SELECT * FROM Tgt_MapCseSmCaseDS