{{ config(materialized='view', tags=['LdMAP_CSE_CRIS_PDCTLkp']) }}

WITH XfmConversions AS (
	SELECT
		{{ ref('SrcMAP_CSE_CRIS_PDCTTera') }}.CRIS_PDCT_ID AS CRIS_PDCT_C,
		ACCT_QLFY_C,
		SRCE_SYST_C
	FROM {{ ref('SrcMAP_CSE_CRIS_PDCTTera') }}
	WHERE 
)

SELECT * FROM XfmConversions