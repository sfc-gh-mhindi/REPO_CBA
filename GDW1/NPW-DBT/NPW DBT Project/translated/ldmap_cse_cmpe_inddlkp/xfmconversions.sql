{{ config(materialized='view', tags=['LdMAP_CSE_CMPE_INDDLkp']) }}

WITH XfmConversions AS (
	SELECT
		INSN_ID,
		CMPE_I
	FROM {{ ref('SrcMAP_CSE_CMPE_IDNNTera') }}
	WHERE 
)

SELECT * FROM XfmConversions