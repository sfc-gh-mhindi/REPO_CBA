{{ config(materialized='view', tags=['LdIntGrupEmplUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_int__grup__empl__u__cse__coi__bus__envi__evnt__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_int__grup__empl__u__cse__coi__bus__envi__evnt__20060101")  }})
TgtIntGrupUpdateDS AS (
	SELECT INT_GRUP_I,
		EMPL_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_int__grup__empl__u__cse__coi__bus__envi__evnt__20060101
)

SELECT * FROM TgtIntGrupUpdateDS