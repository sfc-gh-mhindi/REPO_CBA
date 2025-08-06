{{ config(materialized='view', tags=['LdMAP_CSE_APPT_CLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_APPT_CTera.CCL_APP_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_APPT_CTera') }}.CCL_APP_CAT_ID) AS CCL_APP_CAT_ID,
		-- *SRC*: Trim(InMAP_CSE_APPT_CTera.APPT_C),
		TRIM({{ ref('SrcMAP_CSE_APPT_CTera') }}.APPT_C) AS APPT_C
	FROM {{ ref('SrcMAP_CSE_APPT_CTera') }}
	WHERE 
)

SELECT * FROM XfmConversions