{{ config(materialized='view', tags=['LdUnidPatyGnrc_Ins']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__comm__unid__paty__gnrc AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__comm__unid__paty__gnrc")  }})
dsUnidPatyGnrc AS (
	SELECT UNID_PATY_I,
		EFFT_D,
		PATY_TYPE_C,
		PATY_ROLE_C,
		PROS_KEY_EFFT_I,
		SRCE_SYST_C,
		PATY_QLFY_C,
		SRCE_SYST_PATY_I
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__comm__unid__paty__gnrc
)

SELECT * FROM dsUnidPatyGnrc