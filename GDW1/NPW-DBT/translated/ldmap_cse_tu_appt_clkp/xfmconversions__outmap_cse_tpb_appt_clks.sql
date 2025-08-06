{{ config(materialized='view', tags=['LdMAP_CSE_TU_APPT_CLkp']) }}

WITH XfmConversions__OutMAP_CSE_TPB_APPT_CLks AS (
	SELECT
		{{ ref('SrcMAP_CSE_TU_APPT_CTera') }}.SBTY_CODE AS CHL_TPB_SUBTYPE_CODE,
		-- *SRC*: Trim(InMAP_CSE_TU_APPT_CTera.APPT_C),
		TRIM({{ ref('SrcMAP_CSE_TU_APPT_CTera') }}.APPT_C) AS APPT_C
	FROM {{ ref('SrcMAP_CSE_TU_APPT_CTera') }}
	WHERE 
)

SELECT * FROM XfmConversions__OutMAP_CSE_TPB_APPT_CLks