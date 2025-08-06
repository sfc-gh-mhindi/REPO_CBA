{{ config(materialized='view', tags=['LdPatyRelUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_paty__rel__u__cse__coi__bus__clnt__undtak__20110707 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_paty__rel__u__cse__coi__bus__clnt__undtak__20110707")  }})
TgtPatyRelUpdateDS AS (
	SELECT REL_I,
		PATY_I,
		RELD_PATY_I,
		REL_LEVL_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_paty__rel__u__cse__coi__bus__clnt__undtak__20110707
)

SELECT * FROM TgtPatyRelUpdateDS