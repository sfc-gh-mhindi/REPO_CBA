{{ config(materialized='view', tags=['LdMAP_CSE_APPT_QLFY_LPLkp']) }}

WITH XfmConversions AS (
	SELECT
		LOAN_SBTY_CODE,
		LOAN_APPT_QLFY_C
	FROM {{ ref('SrcMAP_CSE_APPT_QLFYTera') }}
	WHERE 
)

SELECT * FROM XfmConversions