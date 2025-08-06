{{ config(materialized='view', tags=['DltINT_GRUP_UNID_PATYFrmTMP_FA_PROP_CLNT']) }}

WITH XfmCheckDeltaAction__OutTgtIntGrupInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		2 AS DELETE,
		INT_GRUP_I,
		SRCE_SYST_PATY_I,
		ORIG_SRCE_SYST_PATY_I,
		UNID_PATY_M,
		PATY_TYPE_C,
		SRCE_SYST_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.delta_gdw_change_code = INSERT OR {{ ref('JoinAll') }}.delta_gdw_change_code = UPDATE AND {{ ref('JoinAll') }}.CHNG_CODE <> DELETE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtIntGrupInsertDS