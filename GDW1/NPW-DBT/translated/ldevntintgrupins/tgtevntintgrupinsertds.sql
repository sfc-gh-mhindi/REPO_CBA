{{ config(materialized='view', tags=['LdEvntIntGrupIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_evnt__int__grup__i__cse__coi__bus__envi__evnt__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_evnt__int__grup__i__cse__coi__bus__envi__evnt__20060101")  }})
TgtEvntIntGrupInsertDS AS (
	SELECT EVNT_I,
		INT_GRUP_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_evnt__int__grup__i__cse__coi__bus__envi__evnt__20060101
)

SELECT * FROM TgtEvntIntGrupInsertDS