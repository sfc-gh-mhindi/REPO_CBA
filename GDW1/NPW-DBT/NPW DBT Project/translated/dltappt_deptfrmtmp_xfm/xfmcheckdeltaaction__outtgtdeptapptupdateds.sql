{{ config(materialized='view', tags=['DltAppt_DeptFrmTMP_XFM']) }}

WITH XfmCheckDeltaAction__OutTgtDeptApptUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((OutJoin.OLD_DEPT_I)) THEN (OutJoin.OLD_DEPT_I) ELSE ""),
		IFF({{ ref('RmdpFrmInput') }}.OLD_DEPT_I IS NOT NULL, {{ ref('RmdpFrmInput') }}.OLD_DEPT_I, '') AS DEPT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_I)) THEN (OutJoin.NEW_APPT_I) ELSE ""),
		IFF({{ ref('RmdpFrmInput') }}.NEW_APPT_I IS NOT NULL, {{ ref('RmdpFrmInput') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_DEPT_ROLE_C)) THEN (OutJoin.NEW_DEPT_ROLE_C) ELSE ""),
		IFF({{ ref('RmdpFrmInput') }}.NEW_DEPT_ROLE_C IS NOT NULL, {{ ref('RmdpFrmInput') }}.NEW_DEPT_ROLE_C, '') AS DEPT_ROLE_C,
		-- *SRC*: ( IF IsNotNull((OutJoin.OLD_EFFT_D)) THEN (OutJoin.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('RmdpFrmInput') }}.OLD_EFFT_D IS NOT NULL, {{ ref('RmdpFrmInput') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK_MIRR AS PROS_KEY_EXPY_I
	FROM {{ ref('RmdpFrmInput') }}
	WHERE {{ ref('RmdpFrmInput') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtDeptApptUpdateDS