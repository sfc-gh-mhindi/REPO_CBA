{{ config(materialized='view', tags=['LdEvntIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_evnt__i__cse__coi__bus__envi__evnt__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_evnt__i__cse__coi__bus__envi__evnt__20060101")  }})
TgtEvntIInsertDS AS (
	SELECT EVNT_I,
		EVNT_ACTV_TYPE_C,
		BUSN_EVNT_F,
		CTCT_EVNT_F,
		INVT_EVNT_F,
		FNCL_ACCT_EVNT_F,
		FNCL_NVAL_EVNT_F,
		INCD_F,
		INSR_EVNT_F,
		INSR_NVAL_EVNT_F,
		ROW_SECU_ACCS_C,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_evnt__i__cse__coi__bus__envi__evnt__20060101
)

SELECT * FROM TgtEvntIInsertDS