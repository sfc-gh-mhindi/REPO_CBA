{{ config(materialized='view', tags=['DltINT_GRUP_UNID_PATYFrmTMP_FA_PROP_CLNT']) }}

WITH XfmCheckDeltaAction__OutTgtIntGrupUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		2 AS DELETE,
		INT_GRUP_I,
		SRCE_SYST_PATY_I,
		-- *SRC*: ( IF IsNotNull((JoinAllFAPropClnt.OLD_EFFT_D)) THEN (JoinAllFAPropClnt.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.CHNG_CODE = DELETE OR {{ ref('JoinAll') }}.delta_gdw_change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtIntGrupUpdateDS