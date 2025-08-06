{{ config(materialized='view', tags=['LdIntGrupEmplIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_int__grup__empl__i__cse__coi__bus__undtak__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_int__grup__empl__i__cse__coi__bus__undtak__20060101")  }})
TgtIntGrupEmplInsertDS AS (
	SELECT INT_GRUP_I,
		EMPL_I,
		EMPL_ROLE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_int__grup__empl__i__cse__coi__bus__undtak__20060101
)

SELECT * FROM TgtIntGrupEmplInsertDS