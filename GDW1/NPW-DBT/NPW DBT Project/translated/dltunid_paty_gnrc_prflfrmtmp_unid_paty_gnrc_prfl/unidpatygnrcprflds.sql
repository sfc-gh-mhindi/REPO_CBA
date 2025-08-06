{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__unid__paty__gnrc__prfl AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__unid__paty__gnrc__prfl")  }})
UnidPatyGnrcPrflDs AS (
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
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__unid__paty__gnrc__prfl
)

SELECT * FROM UnidPatyGnrcPrflDs