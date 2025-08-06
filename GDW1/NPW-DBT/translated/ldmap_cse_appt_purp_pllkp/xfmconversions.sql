{{ config(materialized='view', tags=['LdMAP_CSE_APPT_PURP_PLLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_APPT_PURP_PLTera.PL_PROD_PURP_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_APPT_PURP_PLTera') }}.PL_PROD_PURP_CAT_ID) AS PL_APP_PROD_PURP_CAT_ID,
		-- *SRC*: Trim(InMAP_CSE_APPT_PURP_PLTera.PURP_TYPE_C),
		TRIM({{ ref('SrcMAP_CSE_APPT_PURP_PLTera') }}.PURP_TYPE_C) AS PURP_TYPE_C
	FROM {{ ref('SrcMAP_CSE_APPT_PURP_PLTera') }}
	WHERE 
)

SELECT * FROM XfmConversions