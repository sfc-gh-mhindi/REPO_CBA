{{ config(materialized='view', tags=['LdMAP_CSE_APPT_QLFYLkp']) }}

WITH XfmConversions__OutMAP_CSE_APPT_QLFYLks AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_APPT_QLFYTera.SBTY_CODE),
		TRIM({{ ref('SrcMAP_CSE_APPT_QLFYTera') }}.SBTY_CODE) AS SBTY_CODE,
		-- *SRC*: Trim(InMAP_CSE_APPT_QLFYTera.APPT_QLFY_C),
		TRIM({{ ref('SrcMAP_CSE_APPT_QLFYTera') }}.APPT_QLFY_C) AS APPT_QLFY_C
	FROM {{ ref('SrcMAP_CSE_APPT_QLFYTera') }}
	WHERE 
)

SELECT * FROM XfmConversions__OutMAP_CSE_APPT_QLFYLks