{{ config(materialized='view', tags=['LdBusnEvntIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_busn__evnt__i__cse__coi__bus__envi__evnt__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_busn__evnt__i__cse__coi__bus__envi__evnt__20060101")  }})
TgtBusnEvntIInsertDS AS (
	SELECT EVNT_I,
		SRCE_SYST_EVNT_I,
		EVNT_ACTL_D,
		SRCE_SYST_C,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		SRCE_SYST_EVNT_TYPE_I,
		EVNT_ACTL_T,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_csel4dev_dataset_busn__evnt__i__cse__coi__bus__envi__evnt__20060101
)

SELECT * FROM TgtBusnEvntIInsertDS