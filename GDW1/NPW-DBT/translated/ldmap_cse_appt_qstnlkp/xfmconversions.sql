{{ config(materialized='view', tags=['LdMAP_CSE_APPT_QSTNLkp']) }}

WITH XfmConversions AS (
	SELECT
		QSTN_ID,
		{{ ref('SrcMAP_CSE_APPT_QSTNTera') }}.ROW_SECU_F AS ROW_S
	FROM {{ ref('SrcMAP_CSE_APPT_QSTNTera') }}
	WHERE 
)

SELECT * FROM XfmConversions