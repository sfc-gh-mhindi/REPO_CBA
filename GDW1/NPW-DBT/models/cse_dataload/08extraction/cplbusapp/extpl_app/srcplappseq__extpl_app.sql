WITH _cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__app__cse__cpl__bus__app__YYYYMMDD AS (
	SELECT *
	
	FROM {{ cvar("files_schema") }}.{{ cvar("base_dir") }}__inprocess__CSE_CPL_BUS_APP_{{ cvar("run_stream") }}_{{ cvar("etl_process_dt_tbl") }}__DLY
	), 
	SrcPlAppSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		DUMMY
	
	FROM _cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__app__cse__cpl__bus__app__YYYYMMDD
	)

SELECT *

FROM SrcPlAppSeq