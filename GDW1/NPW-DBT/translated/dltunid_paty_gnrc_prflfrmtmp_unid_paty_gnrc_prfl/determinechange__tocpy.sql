{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH DetermineChange__ToCpy AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS svExpire,
		1 AS INSERT,
		2 AS DELETE,
		3 AS UPDATE,
		UNID_PATY_I,
		svExpire AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JointoTx') }}
	WHERE {{ ref('JointoTx') }}.change_code = UPDATE OR {{ ref('JointoTx') }}.change_code = DELETE
)

SELECT * FROM DetermineChange__ToCpy