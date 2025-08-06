{{ config(materialized='view', tags=['LdUnidPatyGnrcPrflUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_unid__paty__gnrc__prfl__u__cse__chl__bus__app__20100916 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_unid__paty__gnrc__prfl__u__cse__chl__bus__app__20100916")  }})
TgtUnidPatyGnrcPrflUpdateDS AS (
	SELECT UNID_PATY_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_unid__paty__gnrc__prfl__u__cse__chl__bus__app__20100916
)

SELECT * FROM TgtUnidPatyGnrcPrflUpdateDS