{{ config(materialized='view', tags=['LdTMP_GDW_APPT_PDCTFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__com__bus__ccl__chl__com__app__appt__pdct__activedelete AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__com__bus__ccl__chl__com__app__appt__pdct__activedelete")  }})
SrcApptPdctDS AS (
	SELECT APPT_PDCT_I,
		RELD_APPT_PDCT_I,
		EXPY_FLAG
	FROM _cba__app_csel4_csel4dev_dataset_cse__com__bus__ccl__chl__com__app__appt__pdct__activedelete
)

SELECT * FROM SrcApptPdctDS