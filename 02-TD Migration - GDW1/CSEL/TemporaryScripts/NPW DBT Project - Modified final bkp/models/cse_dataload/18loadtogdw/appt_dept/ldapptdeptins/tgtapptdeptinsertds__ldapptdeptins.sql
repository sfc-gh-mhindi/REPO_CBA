WITH tgtapptdeptinsertds
AS (
	SELECT APPT_I,
		DEPT_ROLE_C,
		EFFT_D,
		DEPT_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	
	FROM {{ cvar("datasets_schema") }}.{{ cvar("base_dir") }}__dataset__{{ cvar("tgt_table_tbl") }}_I_{{ cvar("run_stream") }}_{{ cvar("etl_process_dt") }}__DS
	)

SELECT *

FROM tgtapptdeptinsertds