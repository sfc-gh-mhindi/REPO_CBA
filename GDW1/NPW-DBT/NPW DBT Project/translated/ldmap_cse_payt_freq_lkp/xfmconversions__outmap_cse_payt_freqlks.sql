{{ config(materialized='view', tags=['LdMAP_CSE_PAYT_FREQ_Lkp']) }}

WITH XfmConversions__OutMAP_CSE_PAYT_FREQLks AS (
	SELECT
		{{ ref('SrcMAP_CSE_PAYT_FREQ_Tera') }}.HL_RPAY_PERD_CAT_ID AS REPAY_FREQUENCY_ID,
		PAYT_FREQ_C
	FROM {{ ref('SrcMAP_CSE_PAYT_FREQ_Tera') }}
	WHERE 
)

SELECT * FROM XfmConversions__OutMAP_CSE_PAYT_FREQLks