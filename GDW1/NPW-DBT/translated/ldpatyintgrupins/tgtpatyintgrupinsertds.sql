{{ config(materialized='view', tags=['LdPatyIntGrupIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_paty__int__grup__i__cse__coi__bus__clnt__undtak__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_paty__int__grup__i__cse__coi__bus__clnt__undtak__20060101")  }})
TgtPatyIntGrupInsertDS AS (
	SELECT INT_GRUP_I,
		REL_I,
		SRCE_SYST_C,
		SRCE_SYST_PATY_INT_GRUP_I,
		ORIG_SRCE_SYST_PATY_I,
		ORIG_SRCE_SYST_PATY_TYPE_C,
		PRIM_CLNT_F,
		PATY_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_paty__int__grup__i__cse__coi__bus__clnt__undtak__20060101
)

SELECT * FROM TgtPatyIntGrupInsertDS