{{ config(materialized='view', tags=['DltINT_GRUP_STUSFrmTMP_INT_GRUP_STUS']) }}

WITH XfmCheckDeltaAction__OutTgtIntGrupStusUpdateDS AS (
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
		-- *SRC*: ( IF IsNotNull((JoinAllIntGrupStus.OLD_EFFT_D)) THEN (JoinAllIntGrupStus.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtIntGrupStusUpdateDS