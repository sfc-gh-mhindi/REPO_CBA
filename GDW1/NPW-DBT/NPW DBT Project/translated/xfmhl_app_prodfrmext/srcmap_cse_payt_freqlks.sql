{{ config(materialized='view', tags=['XfmHL_APP_PRODFrmExt']) }}

WITH 
_cba__app_mme_dev_lookupset_map__cse__payt__freq__hl__repayment__period__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_mme_dev_lookupset_map__cse__payt__freq__hl__repayment__period__cat__id")  }})
SrcMAP_CSE_PAYT_FREQLks AS (
	SELECT HL_REPAYMENT_PERIOD_CAT_ID,
		PAYT_FREQ_C
	FROM _cba__app_mme_dev_lookupset_map__cse__payt__freq__hl__repayment__period__cat__id
)

SELECT * FROM SrcMAP_CSE_PAYT_FREQLks