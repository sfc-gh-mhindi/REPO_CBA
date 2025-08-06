{{ config(materialized='view', tags=['LdPhysAdrsUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_phys__adrs__u__cse__chl__bus__prty__adrs__20110701 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_phys__adrs__u__cse__chl__bus__prty__adrs__20110701")  }})
TgtPhysAdrsUpdateDS AS (
	SELECT ADRS_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_phys__adrs__u__cse__chl__bus__prty__adrs__20110701
)

SELECT * FROM TgtPhysAdrsUpdateDS