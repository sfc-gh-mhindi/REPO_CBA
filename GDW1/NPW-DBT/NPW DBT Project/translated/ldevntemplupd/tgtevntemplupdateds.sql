{{ config(materialized='view', tags=['LdEvntEmplUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_evnt__empl__u__cse__coi__bus__envi__evnt__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_evnt__empl__u__cse__coi__bus__envi__evnt__20060101")  }})
TgtEvntEmplUpdateDS AS (
	SELECT EVNT_I,
		EMPL_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_evnt__empl__u__cse__coi__bus__envi__evnt__20060101
)

SELECT * FROM TgtEvntEmplUpdateDS