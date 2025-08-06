{{ config(materialized='view', tags=['DltAPPT_PDCT_RELFrmAPPT_PDCT']) }}

WITH XfmCheckDeltaActionUI AS (
	SELECT
		-- *SRC*: ( IF IsNotNull((OutFilterInsert.APPT_PDCT_I)) THEN (OutFilterInsert.APPT_PDCT_I) ELSE ""),
		IFF({{ ref('FilterStage') }}.APPT_PDCT_I IS NOT NULL, {{ ref('FilterStage') }}.APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((OutFilterInsert.RELD_APPT_PDCT_I)) THEN (OutFilterInsert.RELD_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('FilterStage') }}.RELD_APPT_PDCT_I IS NOT NULL, {{ ref('FilterStage') }}.RELD_APPT_PDCT_I, '') AS RELD_APPT_PDCT_I,
		'PGPD' AS REL_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('FilterStage') }}
	WHERE INSUPD = 'I'
)

SELECT * FROM XfmCheckDeltaActionUI