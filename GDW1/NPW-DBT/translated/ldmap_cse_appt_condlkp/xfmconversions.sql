{{ config(materialized='view', tags=['LdMAP_CSE_APPT_CONDLkp']) }}

WITH XfmConversions AS (
	SELECT
		COND_APPT_CAT_ID,
		APPT_COND_C
	FROM {{ ref('SrcMAP_CSE_APPT_CONDTera') }}
	WHERE 
)

SELECT * FROM XfmConversions