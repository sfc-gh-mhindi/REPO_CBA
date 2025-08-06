{{ config(materialized='view', tags=['LdUnidPatyGnrcPrflIns']) }}

WITH 
_cba__app_csel4_dev_dataset_unid__paty__gnrc__prfl__i__cse__chl__bus__app__20100916 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_unid__paty__gnrc__prfl__i__cse__chl__bus__app__20100916")  }})
TgtTmpUnidPatyGnrcDS AS (
	SELECT UNID_PATY_I,
		SRCE_SYST_C,
		GRDE_C,
		SUB_GRDE_C,
		PRNT_PRVG_F,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_unid__paty__gnrc__prfl__i__cse__chl__bus__app__20100916
)

SELECT * FROM TgtTmpUnidPatyGnrcDS