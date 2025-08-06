{{ config(materialized='view', tags=['LdMAP_CSE_SM_CASE_STUSLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: trim(InMAP_CSE_SM_CASE_STUSTera.SM_STAT_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_SM_CASE_STUSTera') }}.SM_STAT_CAT_ID) AS SM_STATE_CAT_ID,
		-- *SRC*: trim(InMAP_CSE_SM_CASE_STUSTera.STUS_C),
		TRIM({{ ref('SrcMAP_CSE_SM_CASE_STUSTera') }}.STUS_C) AS STUS_C
	FROM {{ ref('SrcMAP_CSE_SM_CASE_STUSTera') }}
	WHERE 
)

SELECT * FROM XfmConversions