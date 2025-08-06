{{ config(materialized='view', tags=['LdMAP_CSE_LOAN_FNDD_METHLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: trim(InMAP_CSE_LOAN_FNDD_METHTera.PL_TARG_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_LOAN_FNDD_METHTera') }}.PL_TARG_CAT_ID) AS PL_TARG_CAT_ID,
		LOAN_FNDD_METH_C
	FROM {{ ref('SrcMAP_CSE_LOAN_FNDD_METHTera') }}
	WHERE 
)

SELECT * FROM XfmConversions