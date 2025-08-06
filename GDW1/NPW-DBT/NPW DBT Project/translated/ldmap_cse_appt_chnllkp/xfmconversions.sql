{{ config(materialized='view', tags=['LdMAP_CSE_APPT_CHNLLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_APPT_ORIGTera.CHNL_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_APPT_ORIGTera') }}.CHNL_CAT_ID) AS CHNL_CAT_ID,
		-- *SRC*: Trim(InMAP_CSE_APPT_ORIGTera.APPT_ORIG_C),
		TRIM({{ ref('SrcMAP_CSE_APPT_ORIGTera') }}.APPT_ORIG_C) AS APPT_ORIG_C
	FROM {{ ref('SrcMAP_CSE_APPT_ORIGTera') }}
	WHERE 
)

SELECT * FROM XfmConversions