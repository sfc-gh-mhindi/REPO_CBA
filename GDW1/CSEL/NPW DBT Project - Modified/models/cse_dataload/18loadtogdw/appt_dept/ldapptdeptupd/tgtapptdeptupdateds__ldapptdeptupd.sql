WITH tgtapptdeptupdateds
AS (
	SELECT DEPT_I,
		APPT_I,
		DEPT_ROLE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	
	FROM {{ cvar("intermediate_db") }}.{{ cvar("datasets_schema") }}.{{ cvar("base_dir") }}__dataset__{{ cvar("tgt_table_tbl") }}_U_{{ cvar("run_stream") }}_{{ cvar("etl_process_dt") }}__DS
	)

SELECT *

FROM tgtapptdeptupdateds