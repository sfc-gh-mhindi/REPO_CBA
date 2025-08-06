{{ config(materialized='view', tags=['DltPATY_INT_GRUPFrmTMP_FA_CLIENT_UNDERTAKING']) }}

WITH XfmCheckDeltaAction__TgtPatyIntGrupUpdatetDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		INT_GRUP_I,
		SRCE_SYST_PATY_INT_GRUP_I,
		{{ ref('JoinAll') }}.OLD_EFFT_D AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__TgtPatyIntGrupUpdatetDS