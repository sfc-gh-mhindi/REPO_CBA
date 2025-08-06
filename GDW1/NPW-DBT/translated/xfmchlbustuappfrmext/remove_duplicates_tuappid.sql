{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH Remove_Duplicates_TuAppid AS (
	SELECT APPT_I, CSE_CPGN_CODE_X, EFFT_D, RUN_STRM 
	FROM (
		SELECT APPT_I, CSE_CPGN_CODE_X, EFFT_D, RUN_STRM,
		 ROW_NUMBER() OVER (PARTITION BY APPT_I ORDER BY 1 ASC) AS ROW_NUM 
		FROM {{ ref('XfmBusinessRules__OutRdTuAppid') }}
	) AS Remove_Duplicates_TuAppid_TEMP
	WHERE ROW_NUM = 1
)

SELECT * FROM Remove_Duplicates_TuAppid