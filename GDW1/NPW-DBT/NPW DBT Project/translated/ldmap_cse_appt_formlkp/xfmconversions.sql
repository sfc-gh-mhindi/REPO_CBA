{{ config(materialized='view', tags=['LdMAP_CSE_APPT_FORMLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_APPT_FORMTera.CCL_FORM_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_APPT_FORMTera') }}.CCL_FORM_CAT_ID) AS CCL_FORM_CAT_ID,
		-- *SRC*: Trim(InMAP_CSE_APPT_FORMTera.APPT_FORM_C),
		TRIM({{ ref('SrcMAP_CSE_APPT_FORMTera') }}.APPT_FORM_C) AS APPT_FORM_C
	FROM {{ ref('SrcMAP_CSE_APPT_FORMTera') }}
	WHERE 
)

SELECT * FROM XfmConversions