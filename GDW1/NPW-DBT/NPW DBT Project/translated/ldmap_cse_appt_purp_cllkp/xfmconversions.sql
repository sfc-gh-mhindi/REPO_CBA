{{ config(materialized='view', tags=['LdMAP_CSE_APPT_PURP_CLLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_APPT_PURP_CLTera.CCL_LOAN_PURP_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_APPT_PURP_CLTera') }}.CCL_LOAN_PURP_CAT_ID) AS CCL_LOAN_PURPOSE_CAT_ID,
		-- *SRC*: Trim(InMAP_CSE_APPT_PURP_CLTera.PURP_TYPE_C),
		TRIM({{ ref('SrcMAP_CSE_APPT_PURP_CLTera') }}.PURP_TYPE_C) AS PURP_TYPE_C
	FROM {{ ref('SrcMAP_CSE_APPT_PURP_CLTera') }}
	WHERE 
)

SELECT * FROM XfmConversions