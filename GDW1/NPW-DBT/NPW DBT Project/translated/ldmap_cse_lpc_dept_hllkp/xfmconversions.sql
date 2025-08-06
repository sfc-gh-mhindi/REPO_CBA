{{ config(materialized='view', tags=['LdMAP_CSE_LPC_DEPT_HLLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_LPC_DEPT_HLTera.LPC_OFFICE),
		TRIM({{ ref('SrcMAP_CSE_LPC_DEPT_HLTera') }}.LPC_OFFICE) AS LPC_OFFICE,
		-- *SRC*: Trim(InMAP_CSE_LPC_DEPT_HLTera.DEPT_I),
		TRIM({{ ref('SrcMAP_CSE_LPC_DEPT_HLTera') }}.DEPT_I) AS DEPT_I
	FROM {{ ref('SrcMAP_CSE_LPC_DEPT_HLTera') }}
	WHERE 
)

SELECT * FROM XfmConversions