{{ config(materialized='view', tags=['DltAPPT_ASETFrmTMP_APPT_ASET']) }}

WITH XfmCheckDeltaAction__OutTgtApptFeatInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		{{ ref('JoinAll') }}.NEW_APPT_I AS APPT_I,
		{{ ref('JoinAll') }}.NEW_ASET_I AS ASET_I,
		{{ ref('JoinAll') }}.NEW_PRIM_SECU_F AS PRIM_SECU_F,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS efft_d,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS expy_d,
		REFR_PK AS pros_key_efft_i,
		-- *SRC*: setnull(),
		SETNULL() AS pros_key_expy_i,
		-- *SRC*: setnull(),
		SETNULL() AS eror_seqn_i
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptFeatInsertDS