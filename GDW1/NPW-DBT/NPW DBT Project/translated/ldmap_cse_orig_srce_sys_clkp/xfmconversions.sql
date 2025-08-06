{{ config(materialized='view', tags=['LdMAP_CSE_ORIG_SRCE_SYS_CLkp']) }}

WITH XfmConversions AS (
	SELECT
		{{ ref('SrcMAP_CSE_ORIG_SRCE_SYS_CTera') }}.orig_srce_syst_i AS ORIG_SRCE_SYST_I,
		{{ ref('SrcMAP_CSE_ORIG_SRCE_SYS_CTera') }}.ORIG_APPT_SRCE_C AS ORIG_SRCE_SYST_C
	FROM {{ ref('SrcMAP_CSE_ORIG_SRCE_SYS_CTera') }}
	WHERE 
)

SELECT * FROM XfmConversions