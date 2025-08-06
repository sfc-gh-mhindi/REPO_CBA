{{ config(materialized='view', tags=['DltAPPT_PDCT_RELFrmAPPT_PDCT']) }}

WITH XfmCheckDeltaActionU AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		-- *SRC*: ( IF IsNotNull((OutFilterDelete.APPT_PDCT_I)) THEN (OutFilterDelete.APPT_PDCT_I) ELSE ""),
		IFF({{ ref('FilterStage') }}.APPT_PDCT_I IS NOT NULL, {{ ref('FilterStage') }}.APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((OutFilterDelete.RELD_APPT_PDCT_I)) THEN (OutFilterDelete.RELD_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('FilterStage') }}.RELD_APPT_PDCT_I IS NOT NULL, {{ ref('FilterStage') }}.RELD_APPT_PDCT_I, '') AS RELD_APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((OutFilterDelete.EFFT_D)) THEN (OutFilterDelete.EFFT_D) ELSE ""),
		IFF({{ ref('FilterStage') }}.EFFT_D IS NOT NULL, {{ ref('FilterStage') }}.EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('FilterStage') }}
	WHERE INSUPD = 'U'
)

SELECT * FROM XfmCheckDeltaActionU