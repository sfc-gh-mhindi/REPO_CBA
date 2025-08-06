{{ config(materialized='view', tags=['DltAPPT_PDCT_RELFrmAPPT_PDCT_2']) }}

WITH XfmCheckDeltaActionU AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		-- *SRC*: ( IF IsNotNull((InApptPdctRelTera.APPT_PDCT_I)) THEN (InApptPdctRelTera.APPT_PDCT_I) ELSE ""),
		IFF({{ ref('SrcApptPdctRelTera') }}.APPT_PDCT_I IS NOT NULL, {{ ref('SrcApptPdctRelTera') }}.APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((InApptPdctRelTera.RELD_APPT_PDCT_I)) THEN (InApptPdctRelTera.RELD_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('SrcApptPdctRelTera') }}.RELD_APPT_PDCT_I IS NOT NULL, {{ ref('SrcApptPdctRelTera') }}.RELD_APPT_PDCT_I, '') AS RELD_APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((InApptPdctRelTera.EFFT_D)) THEN (InApptPdctRelTera.EFFT_D) ELSE ""),
		IFF({{ ref('SrcApptPdctRelTera') }}.EFFT_D IS NOT NULL, {{ ref('SrcApptPdctRelTera') }}.EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcApptPdctRelTera') }}
	WHERE 
)

SELECT * FROM XfmCheckDeltaActionU