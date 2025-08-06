{{ config(materialized='view', tags=['LdUnidPatyIdnnGnrcIns']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__chl__bus__app__comm__unid__paty__idnn__gnrc__i__20110623 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__chl__bus__app__comm__unid__paty__idnn__gnrc__i__20110623")  }})
UNID_PATY_IDNN_GNRC_INSERT AS (
	SELECT SRCE_SYST_C,
		IDNN_TYPE_C,
		IDNN_VALU_X,
		EFFT_D,
		EXPY_D,
		UNID_PATY_I,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_cse__chl__bus__app__comm__unid__paty__idnn__gnrc__i__20110623
)

SELECT * FROM UNID_PATY_IDNN_GNRC_INSERT