{{ config(materialized='view', tags=['LdMAP_CSE_CMS_PDCTLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CMS_PDCTTera.CRIS_PDCT_CAT_ID),
		TRIM({{ ref('SrcMAP_CMS_PDCTTera') }}.CRIS_PDCT_CAT_ID) AS CRIS_PDCT_CAT_ID,
		-- *SRC*: Trim(InMAP_CMS_PDCTTera.CRIS_PDCT_C),
		TRIM({{ ref('SrcMAP_CMS_PDCTTera') }}.CRIS_PDCT_C) AS CRIS_PDCT_C,
		-- *SRC*: Trim(InMAP_CMS_PDCTTera.CRIS_DESC),
		TRIM({{ ref('SrcMAP_CMS_PDCTTera') }}.CRIS_DESC) AS CRIS_DESC,
		-- *SRC*: Trim(InMAP_CMS_PDCTTera.ACCT_I_PRFX),
		TRIM({{ ref('SrcMAP_CMS_PDCTTera') }}.ACCT_I_PRFX) AS ACCT_I_PRFX
	FROM {{ ref('SrcMAP_CMS_PDCTTera') }}
	WHERE 
)

SELECT * FROM XfmConversions