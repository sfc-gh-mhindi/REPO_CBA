{{ config(materialized='view', tags=['XfmHL_APPFrmExt']) }}

WITH DeDup_CseApptHlCondAppr AS (
	SELECT APPT_I, EFFT_D, COND_APPR_F, COND_APPR_CONV_TO_FULL_D, EXPY_D, ROW_SECU_ACCS_C 
	FROM (
		SELECT APPT_I, EFFT_D, COND_APPR_F, COND_APPR_CONV_TO_FULL_D, EXPY_D, ROW_SECU_ACCS_C,
		 ROW_NUMBER() OVER (PARTITION BY APPT_I ORDER BY 1 ASC) AS ROW_NUM 
		FROM {{ ref('XfmBusinessRules__OutTmpCseApptHlCondAppr') }}
	) AS DeDup_CseApptHlCondAppr_TEMP
	WHERE ROW_NUM = 1
)

SELECT * FROM DeDup_CseApptHlCondAppr