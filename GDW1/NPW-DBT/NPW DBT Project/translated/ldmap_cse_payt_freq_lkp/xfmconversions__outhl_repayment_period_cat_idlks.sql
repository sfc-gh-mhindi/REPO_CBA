{{ config(materialized='view', tags=['LdMAP_CSE_PAYT_FREQ_Lkp']) }}

WITH XfmConversions__OutHL_REPAYMENT_PERIOD_CAT_IDLks AS (
	SELECT
		{{ ref('SrcMAP_CSE_PAYT_FREQ_Tera') }}.HL_RPAY_PERD_CAT_ID AS HL_REPAYMENT_PERIOD_CAT_ID,
		PAYT_FREQ_C
	FROM {{ ref('SrcMAP_CSE_PAYT_FREQ_Tera') }}
	WHERE 
)

SELECT * FROM XfmConversions__OutHL_REPAYMENT_PERIOD_CAT_IDLks