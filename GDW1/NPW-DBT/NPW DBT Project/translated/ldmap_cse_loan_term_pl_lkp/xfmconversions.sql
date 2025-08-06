{{ config(materialized='view', tags=['LdMAP_CSE_LOAN_TERM_PL_Lkp']) }}

WITH XfmConversions AS (
	SELECT
		PL_PROD_TERM_CAT_ID,
		{{ ref('SrcMAP_CSE_LOAN_TERM_PL_Tera') }}.ACTL_VALU_Q AS SRCE_SYST_ACTL_TERM_Q
	FROM {{ ref('SrcMAP_CSE_LOAN_TERM_PL_Tera') }}
	WHERE 
)

SELECT * FROM XfmConversions