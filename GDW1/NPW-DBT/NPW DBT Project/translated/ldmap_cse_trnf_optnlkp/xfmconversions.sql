{{ config(materialized='view', tags=['LdMAP_CSE_TRNF_OPTNLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: trim(InMAP_CSE_TRNF_OPTNTera.BAL_XFER_OPTN_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_TRNF_OPTNTera') }}.BAL_XFER_OPTN_CAT_ID) AS BAL_XFER_OPTION_CAT_ID,
		-- *SRC*: trim(InMAP_CSE_TRNF_OPTNTera.TRNF_OPTN_C),
		TRIM({{ ref('SrcMAP_CSE_TRNF_OPTNTera') }}.TRNF_OPTN_C) AS TRNF_OPTN_C
	FROM {{ ref('SrcMAP_CSE_TRNF_OPTNTera') }}
	WHERE 
)

SELECT * FROM XfmConversions