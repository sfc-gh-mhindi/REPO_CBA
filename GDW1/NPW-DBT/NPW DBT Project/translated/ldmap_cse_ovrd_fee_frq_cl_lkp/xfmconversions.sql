{{ config(materialized='view', tags=['LdMAP_CSE_OVRD_FEE_FRQ_CL_Lkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: trim(InMAP_CSE_FEAT_FEE_PLTera.OVRD_FEE_PCT_FREQ),
		TRIM({{ ref('SrcMAP_CSE_OVRD_FEE_FRQ_CLTera') }}.OVRD_FEE_PCT_FREQ) AS OVRD_FEE_PCT_FREQ,
		FREQ_IN_MTHS
	FROM {{ ref('SrcMAP_CSE_OVRD_FEE_FRQ_CLTera') }}
	WHERE 
)

SELECT * FROM XfmConversions