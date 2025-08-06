{{ config(materialized='view', tags=['XfmPL_APP_PRODFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__payt__freq__repay__frequency__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__payt__freq__repay__frequency__id")  }})
SrcMAP_CSE_PAYT_FREQ_Lks AS (
	SELECT REPAY_FREQUENCY_ID,
		PAYT_FREQ_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__payt__freq__repay__frequency__id
)

SELECT * FROM SrcMAP_CSE_PAYT_FREQ_Lks