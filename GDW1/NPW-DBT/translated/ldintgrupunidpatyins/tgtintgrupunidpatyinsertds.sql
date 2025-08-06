{{ config(materialized='view', tags=['LdIntGrupUnidPatyIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_int__grup__unid__paty__i__cse__coi__bus__prop__clnt__20061016 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_int__grup__unid__paty__i__cse__coi__bus__prop__clnt__20061016")  }})
TgtIntGrupUnidPatyInsertDS AS (
	SELECT INT_GRUP_I,
		SRCE_SYST_PATY_I,
		ORIG_SRCE_SYST_PATY_I,
		UNID_PATY_M,
		PATY_TYPE_C,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_int__grup__unid__paty__i__cse__coi__bus__prop__clnt__20061016
)

SELECT * FROM TgtIntGrupUnidPatyInsertDS