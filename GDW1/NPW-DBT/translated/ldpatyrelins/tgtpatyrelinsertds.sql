{{ config(materialized='view', tags=['LdPatyRelIns']) }}

WITH 
_cba__app_csel4_dev_dataset_paty__rel__i__cse__coi__bus__clnt__undtak__20110707 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_paty__rel__i__cse__coi__bus__clnt__undtak__20110707")  }})
TgtPatyRelInsertDS AS (
	SELECT PATY_I,
		RELD_PATY_I,
		REL_I,
		REL_TYPE_C,
		SRCE_SYST_C,
		PATY_ROLE_C,
		REL_STUS_C,
		REL_REAS_C,
		REL_LEVL_C,
		REL_EFFT_D,
		REL_EXPY_D,
		SRCE_SYST_REL_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_paty__rel__i__cse__coi__bus__clnt__undtak__20110707
)

SELECT * FROM TgtPatyRelInsertDS