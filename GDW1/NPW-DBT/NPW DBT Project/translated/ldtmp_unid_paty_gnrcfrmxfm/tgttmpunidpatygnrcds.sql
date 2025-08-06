{{ config(materialized='view', tags=['LdTMP_UNID_PATY_GNRCFrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__unid__paty__gnrc AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__unid__paty__gnrc")  }})
TgtTmpUnidPatyGnrcDS AS (
	SELECT UNID_PATY_I,
		RUN_STRM,
		SRCE_SYST_PATY_I
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__unid__paty__gnrc
)

SELECT * FROM TgtTmpUnidPatyGnrcDS