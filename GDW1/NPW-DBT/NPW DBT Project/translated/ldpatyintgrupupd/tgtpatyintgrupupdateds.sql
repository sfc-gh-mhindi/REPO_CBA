{{ config(materialized='view', tags=['LdPatyIntGrupUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_paty__int__grup__u__cse__coi__bus__clnt__undtak__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_paty__int__grup__u__cse__coi__bus__clnt__undtak__20060101")  }})
TgtPatyIntGrupUpdateDS AS (
	SELECT INT_GRUP_I,
		SRCE_SYST_PATY_INT_GRUP_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_paty__int__grup__u__cse__coi__bus__clnt__undtak__20060101
)

SELECT * FROM TgtPatyIntGrupUpdateDS