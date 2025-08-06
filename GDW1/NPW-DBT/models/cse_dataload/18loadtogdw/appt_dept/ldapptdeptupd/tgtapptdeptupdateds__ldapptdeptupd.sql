WITH source
AS (
	SELECT *
	
	FROM {{ cvar("datasets_schema") }}.{{ cvar("base_dir") }}__dataset__{{ cvar("tgt_table_tbl") }}_U_{{ cvar("run_stream") }}_{{ cvar("etl_process_dt_tbl") }}__DS
	),
TgtApptDeptUpdateDS
AS (
	SELECT DEPT_I,
		APPT_I,
		DEPT_ROLE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	
	FROM source
	)

SELECT *

FROM TgtApptDeptUpdateDS