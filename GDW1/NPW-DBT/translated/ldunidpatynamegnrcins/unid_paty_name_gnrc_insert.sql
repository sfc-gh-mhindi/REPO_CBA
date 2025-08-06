{{ config(materialized='view', tags=['LdUnidPatyNameGnrcIns']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__chl__bus__app__com__det__unid__paty__name__gnrc__i20110623 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__chl__bus__app__com__det__unid__paty__name__gnrc__i20110623")  }})
UNID_PATY_NAME_GNRC_INSERT AS (
	SELECT UNID_PATY_I,
		PATY_NAME_TYPE_C,
		EFFT_D,
		SRCE_SYST_C,
		SALU_C,
		PATY_ROLE_C,
		TITL_C,
		FRST_M,
		SCND_M,
		SRNM_M,
		THRD_M,
		FRTH_M,
		SUF_C,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		CO_CTCT_FRST_M,
		CO_CTCT_LAST_M,
		CO_CTCT_PRFR_M
	FROM _cba__app_csel4_dev_dataset_cse__chl__bus__app__com__det__unid__paty__name__gnrc__i20110623
)

SELECT * FROM UNID_PATY_NAME_GNRC_INSERT