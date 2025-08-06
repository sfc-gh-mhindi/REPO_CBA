{{ config(materialized='view', tags=['LdUnidPatyGnrcIns']) }}

WITH 
_cba__app_csel4_dev_dataset_unid__paty__gnrc__i__cse__chl__bus__app__20100914 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_unid__paty__gnrc__i__cse__chl__bus__app__20100914")  }})
TgTUnidPatyGnrcDS AS (
	SELECT UNID_PATY_I,
		EFFT_D,
		PATY_TYPE_C,
		PATY_ROLE_C,
		PROS_KEY_EFFT_I,
		SRCE_SYST_C,
		PATY_QLFY_C,
		SRCE_SYST_PATY_I
	FROM _cba__app_csel4_dev_dataset_unid__paty__gnrc__i__cse__chl__bus__app__20100914
)

SELECT * FROM TgTUnidPatyGnrcDS