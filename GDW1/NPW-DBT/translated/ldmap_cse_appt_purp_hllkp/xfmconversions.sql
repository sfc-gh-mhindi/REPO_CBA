{{ config(materialized='view', tags=['LdMAP_CSE_APPT_PURP_HLLkp']) }}

WITH XfmConversions AS (
	SELECT
		{{ ref('SrcMAP_CSE_APPT_PURP_HLTera') }}.HL_LOAN_PURP_CAT_ID AS HL_LOAN_PURPOSE_CAT_ID,
		PURP_TYPE_C
	FROM {{ ref('SrcMAP_CSE_APPT_PURP_HLTera') }}
	WHERE 
)

SELECT * FROM XfmConversions