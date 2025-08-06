{{ config(materialized='view', tags=['DltINT_GRUP_STUSFrmTMP_INT_GRUP_STUS']) }}

WITH XfmCheckDeltaAction__OutTgtIntGrupStusInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllIntGrupStus.NEW_INT_GRUP_I)) THEN (JoinAllIntGrupStus.NEW_INT_GRUP_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_INT_GRUP_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_INT_GRUP_I, '') AS INT_GRUP_I,
		-- *SRC*: ( IF IsNotNull((JoinAllIntGrupStus.NEW_STRT_S)) THEN (JoinAllIntGrupStus.NEW_STRT_S) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_STRT_S IS NOT NULL, {{ ref('JoinAll') }}.NEW_STRT_S, '') AS STRT_S,
		-- *SRC*: ( IF IsNotNull((JoinAllIntGrupStus.NEW_STUS_C)) THEN (JoinAllIntGrupStus.NEW_STUS_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_STUS_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_STUS_C, '') AS STUS_C,
		-- *SRC*: ( IF IsNotNull((JoinAllIntGrupStus.NEW_STRT_D)) THEN (JoinAllIntGrupStus.NEW_STRT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_STRT_D IS NOT NULL, {{ ref('JoinAll') }}.NEW_STRT_D, '') AS STRT_D,
		-- *SRC*: ( IF IsNotNull((JoinAllIntGrupStus.NEW_STRT_T)) THEN (JoinAllIntGrupStus.NEW_STRT_T) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_STRT_T IS NOT NULL, {{ ref('JoinAll') }}.NEW_STRT_T, '') AS STRT_T,
		{{ ref('JoinAll') }}.NEW_EMPL_I AS EMPL_I,
		{{ ref('JoinAll') }}.NEW_END_S AS END_S,
		{{ ref('JoinAll') }}.NEW_END_D AS END_D,
		{{ ref('JoinAll') }}.NEW_END_T AS END_T,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtIntGrupStusInsertDS