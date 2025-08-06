{{ config(materialized='view', tags=['LdMAP_CSE_JOB_COMM_CATG_LPLkp']) }}

WITH XfmConversions AS (
	SELECT
		CLP_JOB_FAMILY_CAT_ID,
		JOB_COMM_CATG_C
	FROM {{ ref('SrcMAP_CSE_APPT_QLFYTera') }}
	WHERE 
)

SELECT * FROM XfmConversions