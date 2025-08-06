{{ config(materialized='view', tags=['LdIntGrupUnidPatyUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_int__grup__unid__paty__u__cse__coi__bus__prop__clnt__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_int__grup__unid__paty__u__cse__coi__bus__prop__clnt__20060101")  }})
TgtIntGrupUnidPatyUpdateDS AS (
	SELECT INT_GRUP_I,
		SRCE_SYST_PATY_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_int__grup__unid__paty__u__cse__coi__bus__prop__clnt__20060101
)

SELECT * FROM TgtIntGrupUnidPatyUpdateDS