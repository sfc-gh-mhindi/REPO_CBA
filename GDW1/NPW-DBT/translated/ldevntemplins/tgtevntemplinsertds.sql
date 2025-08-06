{{ config(materialized='view', tags=['LdEvntEmplIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_evnt__empl__i__cse__coi__bus__envi__evnt__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_evnt__empl__i__cse__coi__bus__envi__evnt__20060101")  }})
TgtEvntEmplInsertDS AS (
	SELECT EVNT_I,
		ROW_SECU_ACCS_C,
		EMPL_I,
		EVNT_PATY_ROLE_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_evnt__empl__i__cse__coi__bus__envi__evnt__20060101
)

SELECT * FROM TgtEvntEmplInsertDS