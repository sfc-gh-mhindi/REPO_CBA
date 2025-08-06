{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH DetermineChange__ToInserts AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS svExpire,
		1 AS INSERT,
		2 AS DELETE,
		3 AS UPDATE,
		UNID_PATY_I,
		SRCE_SYST_C,
		GRDE_C,
		SUB_GRDE_C,
		PRNT_PRVG_F,
		EFFT_D,
		EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('JointoTx') }}
	WHERE {{ ref('JointoTx') }}.change_code = INSERT OR {{ ref('JointoTx') }}.change_code = UPDATE
)

SELECT * FROM DetermineChange__ToInserts