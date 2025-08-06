{{ config(materialized='view', tags=['LdMAP_CSE_SM_CASE_Lukp_Ld']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: trim(InMAP_CSE_SM_CASE.SM_CASE_ID),
		TRIM({{ ref('SrcMAP_CSE_SM_CASEOra') }}.SM_CASE_ID) AS SM_CASE_ID,
		-- *SRC*: trim(InMAP_CSE_SM_CASE.TARG_I),
		TRIM({{ ref('SrcMAP_CSE_SM_CASEOra') }}.TARG_I) AS TARG_I,
		-- *SRC*: trim(InMAP_CSE_SM_CASE.TARG_SUBJ),
		TRIM({{ ref('SrcMAP_CSE_SM_CASEOra') }}.TARG_SUBJ) AS TARG_SUBJ
	FROM {{ ref('SrcMAP_CSE_SM_CASEOra') }}
	WHERE 
)

SELECT * FROM XfmConversions