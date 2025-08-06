{{ config(materialized='view', tags=['LdTMP_APPT_PRMO_TRAKFrmXfm']) }}

WITH fn_CombInsDelRecds AS (
	SELECT
		APPT_PDCT_I as APPT_PDCT_I,
		TRAK_I as TRAK_I,
		MOD_DATE as MOD_DATE,
		RECD_IND as RECD_IND
	FROM {{ ref('xf_AddModDate') }}
	UNION ALL
	SELECT
		APPT_PDCT_I,
		TRAK_I,
		MOD_DATE,
		RECD_IND
	FROM {{ ref('xf_TransInsRecds') }}
)

SELECT * FROM fn_CombInsDelRecds