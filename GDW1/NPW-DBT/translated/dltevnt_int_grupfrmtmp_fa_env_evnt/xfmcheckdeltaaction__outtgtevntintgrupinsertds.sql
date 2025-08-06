{{ config(materialized='view', tags=['DltEVNT_INT_GRUPFrmTMP_FA_ENV_EVNT']) }}

WITH XfmCheckDeltaAction__OutTgtEvntIntGrupInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		'N' AS UpdateFlag,
		-- *SRC*: ( IF IsNotNull((OutChangeCapture.INT_GRUP_I)) THEN (OutChangeCapture.INT_GRUP_I) ELSE ""),
		IFF({{ ref('ChangeCapture') }}.INT_GRUP_I IS NOT NULL, {{ ref('ChangeCapture') }}.INT_GRUP_I, '') AS INT_GRUP_I,
		-- *SRC*: ( IF IsNotNull((OutChangeCapture.EVNT_I)) THEN (OutChangeCapture.EVNT_I) ELSE ""),
		IFF({{ ref('ChangeCapture') }}.EVNT_I IS NOT NULL, {{ ref('ChangeCapture') }}.EVNT_I, '') AS EVNT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('ChangeCapture') }}
	WHERE {{ ref('ChangeCapture') }}.change_code = INSERT
)

SELECT * FROM XfmCheckDeltaAction__OutTgtEvntIntGrupInsertDS