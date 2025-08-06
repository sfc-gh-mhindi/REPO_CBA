{{ config(materialized='view', tags=['XfmHL_APPFrmExt']) }}

WITH DeDup_CseApptCpgn AS (
	SELECT APPT_I, CSE_CPGN_CODE_X, EFFT_D, RUN_STRM 
	FROM (
		SELECT APPT_I, CSE_CPGN_CODE_X, EFFT_D, RUN_STRM,
		 ROW_NUMBER() OVER (PARTITION BY APPT_I ORDER BY 1 ASC) AS ROW_NUM 
		FROM {{ ref('XfmBusinessRules__OutTmpCseApptCpgn') }}
	) AS DeDup_CseApptCpgn_TEMP
	WHERE ROW_NUM = 1
)

SELECT * FROM DeDup_CseApptCpgn